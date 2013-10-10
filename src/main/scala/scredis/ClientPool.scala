/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package scredis

import com.typesafe.config.Config

import akka.dispatch.ExecutionContext
import akka.util.{ Duration, FiniteDuration }

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.commons.pool.impl.GenericObjectPool.{ Config => PoolConfig }

import scredis.exceptions.{ RedisCommandException, RedisConnectionException }

import java.util.concurrent.{ ExecutorService, Executors }
import java.util.concurrent.locks.ReentrantReadWriteLock

private[scredis] final class ClientFactory private[scredis] (
  host: String,
  port: Int,
  password: Option[String],
  database: Int,
  timeout: Duration,
  tries: Int,
  sleep: Option[FiniteDuration],
  validateWithPing: Boolean
) extends BasePoolableObjectFactory[Client] {
  
  override def makeObject = try {
    new Client(host, port, password, database, timeout, tries, sleep)
  } catch {
    case e: Throwable => throw RedisConnectionException(
      e,
      Some("Could not connect to %s:%d"),
      host,
      port
    )
  }
  
  override def destroyObject(client: Client): Unit = client.quit()
  
  override def validateObject(client: Client) = (
    client.isConnected &&
    (!validateWithPing || client.ping() == "PONG")
  )
  
}

/**
 * Configurable pool of clients to be used in a multi-threaded environment.
 * 
 * @param config apache pool config (org.apache.commons.pool.impl.GenericObjectPool.Config)
 * @param testWithPing when `true`, internal clients are validated with a PING command,
 * when false, only socket information are used (less reliable but more performant)
 * @param host server address
 * @param port server port
 * @param password server password
 * @param database database index to select
 * @param timeout maximum duration for the execution of a command, can be infinite
 * @param tries maximum number of times the command will be executed in case recoverable errors
 * occur such as timeouts
 * @param sleep duration of time to sleep between subsequent tries
 * 
 * @define none `None`
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define pool [[scredis.ClientPool]]
 * @define tc com.typesafe.Config
 */
final class ClientPool(
  config: PoolConfig = RedisConfigDefaults.Pool.Config,
  testWithPing: Boolean = RedisConfigDefaults.Pool.TestWithPing
)(
  host: String = RedisConfigDefaults.Client.Host,
  port: Int = RedisConfigDefaults.Client.Port,
  private var password: Option[String] = RedisConfigDefaults.Client.Password,
  database: Int = RedisConfigDefaults.Client.Database,
  timeout: Duration = RedisConfigDefaults.Client.Timeout,
  tries: Int = RedisConfigDefaults.Client.Tries,
  sleep: Option[FiniteDuration] = RedisConfigDefaults.Client.Sleep
) {
  
  /**
   * Constructs a $pool instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $pool
   */
  def this(config: RedisConfig) = this(config.Pool.Config, config.Pool.TestWithPing)(
    config.Client.Host,
    config.Client.Port,
    config.Client.Password,
    config.Client.Database,
    config.Client.Timeout,
    config.Client.Tries,
    config.Client.Sleep
  )
  
  /**
   * Constructs a $pool instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new ClientPool(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $pool
   */
  def this(config: Config) = this(RedisConfig(config))
  
  /**
   * Constructs a $pool instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $pool
   */
  def this(config: Config, path: String) = this(RedisConfig(config, path))
  
  /**
   * Constructs a $pool instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new ClientPool(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $pool
   */
  def this(configName: String) = this(RedisConfig(configName))
  
  /**
   * Constructs a $pool instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $pool
   */
  def this(configName: String, path: String) = this(RedisConfig(configName, path))

  private val factory = new ClientFactory(
    host,
    port,
    password,
    database,
    timeout,
    tries,
    sleep,
    testWithPing
  )
  
  private val lock = new ReentrantReadWriteLock()
  private var pool = new GenericObjectPool(factory, config)
  
  private def withReadLock[A](f: => A): A = {
    lock.readLock.lock()
    try {
      f
    } finally {
      lock.readLock.unlock()
    }
  }
  
  private def withWriteLock[A](f: => A): A = {
    lock.writeLock.lock()
    try {
      f
    } finally {
      lock.writeLock.unlock()
    }
  }
  
  /**
   * Borrows a `Client` from the pool
   * 
   * @note this can throw an exception depending on how it is configured, e.g. if bounded and 
   * non-blocking
   * 
   * @return the borrowed `Client`
   */
  def borrowClient(): Client = withReadLock {
    pool.borrowObject()
  }
  
  /**
   * Returns a borrowed `Client` to the pool
   */
  def returnClient(client: Client): Unit = pool.returnObject(client)

  def withClient[A](body: Client => A): A = withReadLock {
    val client = borrowClient()
    try {
      body(client)
    } finally {
      returnClient(client)
    }
  }
  
  /**
   * Changes the selected database on all clients.
   * 
   * @note if the provided database index is invalid then previously set index remains.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(db: Int)(implicit opts: CommandOptions = DefaultCommandOptions): Unit = {
    // Validate database
    withClient(_.select(db))
    
    val newFactory = new ClientFactory(
      host,
      port,
      password,
      db,
      timeout,
      tries,
      sleep,
      testWithPing
    )
    withWriteLock {
      pool.close()
      pool = new GenericObjectPool(newFactory, config)
    }
  }
  
  /**
   * Authenticates/Deauthenticates all clients to/from the server.
   * 
   * @note If the provided password is incorrect, then whatever was set as the password before
   * remains.
   *
   * @param passwordOpt the server password, or $none if the server does not require any
   * @throws $e if the password is incorrect
   *
   * @since 1.0.0
   */
  def auth(passwordOpt: Option[String])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = {
    
    // Validate password
    passwordOpt.foreach { newPassword =>
      withClient { client =>
        this.password match {
          case Some(currentPassword) => try {
            client.auth(newPassword)
          } catch {
            case e: RedisCommandException => {
              client.auth(currentPassword)
              throw e
            }
          }
          case None => client.auth(newPassword)
        }
      }
    }
    
    this.password = passwordOpt
    
    val newFactory = new ClientFactory(
      host,
      port,
      passwordOpt,
      database,
      timeout,
      tries,
      sleep,
      testWithPing
    )
    withWriteLock {
      pool.close()
      pool = new GenericObjectPool(newFactory, config)
    }
  }
  
  /**
   * Shutdowns the pool as well as every contained `Client`.
   */
  def close(): Unit = withWriteLock {
    pool.close()
  }

}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define pool [[scredis.ClientPool]]
 * @define tc com.typesafe.Config
 */
object ClientPool {
  
  /**
   * Creates a $pool
   * 
   * @param config apache pool config (org.apache.commons.pool.impl.GenericObjectPool.Config)
   * @param testWithPing when `true`, internal clients are validated with a PING command,
   * when false, only socket information are used (less reliable but more performant)
   * @param host server address
   * @param port server port
   * @param password server password
   * @param database database index to select
   * @param timeout maximum duration for the execution of a command, can be infinite
   * @param tries maximum number of times the command will be executed in case recoverable errors
   * occur such as timeouts
   * @param sleep duration of time to sleep between subsequent tries
   */
  def apply(
    config: PoolConfig = RedisConfigDefaults.Pool.Config,
    testWithPing: Boolean = RedisConfigDefaults.Pool.TestWithPing
  )(
    host: String = RedisConfigDefaults.Client.Host,
    port: Int = RedisConfigDefaults.Client.Port,
    password: Option[String] = RedisConfigDefaults.Client.Password,
    database: Int = RedisConfigDefaults.Client.Database,
    timeout: Duration = RedisConfigDefaults.Client.Timeout,
    tries: Int = RedisConfigDefaults.Client.Tries,
    sleep: Option[FiniteDuration] = RedisConfigDefaults.Client.Sleep
  ) = new ClientPool(
    config,
    testWithPing
  )(
    host,
    port,
    password,
    database,
    timeout,
    tries,
    sleep
  )
  
  /**
   * Constructs a $pool instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $pool
   */
  def apply(config: RedisConfig): ClientPool = new ClientPool(config)
  
  /**
   * Constructs a $pool instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * ClientPool(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $pool
   */
  def apply(config: Config) = new ClientPool(config)
  
  /**
   * Constructs a $pool instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $pool
   */
  def apply(config: Config, path: String) = new ClientPool(config, path)
  
  /**
   * Constructs a $pool instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * ClientPool(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $pool
   */
  def apply(configName: String): ClientPool = new ClientPool(configName)
  
  /**
   * Constructs a $pool instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $pool
   */
  def apply(configName: String, path: String): ClientPool = new ClientPool(configName, path)
  
}