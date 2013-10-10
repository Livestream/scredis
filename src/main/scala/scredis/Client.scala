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

import scredis.io.JavaSocketConnection
import scredis.protocol.Protocol
import scredis.commands._
import scredis.exceptions._

import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
 * Defines a basic Redis client supporting all commands.
 * 
 * @note a Client is '''not''' thread-safe. To execute commands within multiple threads, use
 * [[scredis.Redis]] or [[scredis.ClientPool]].
 * 
 * @param host server address
 * @param port server port
 * @param password server password
 * @param database database index to select
 * @param timeout maximum duration for the execution of a command, can be infinite
 * @param tries maximum number of times the command will be executed in case recoverable errors
 * occur such as timeouts
 * @param sleep duration of time to sleep between subsequent tries
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define client [[scredis.Client]]
 * @define tc com.typesafe.Config
 */
final class Client(
  host: String = RedisConfigDefaults.Client.Host,
  port: Int = RedisConfigDefaults.Client.Port,
  private var password: Option[String] = RedisConfigDefaults.Client.Password,
  private var database: Int = RedisConfigDefaults.Client.Database,
  timeout: Duration = RedisConfigDefaults.Client.Timeout,
  tries: Int = RedisConfigDefaults.Client.Tries,
  sleep: Option[FiniteDuration] = RedisConfigDefaults.Client.Sleep
) extends Protocol
  with ConnectionCommands
  with ServerCommands
  with KeysCommands
  with StringsCommands
  with HashesCommands
  with ListsCommands
  with SetsCommands
  with SortedSetsCommands
  with ScriptingCommands
  with PubSubCommands
  with TransactionalCommands {
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def this(config: RedisConfig) = this(
    config.Client.Host,
    config.Client.Port,
    config.Client.Password,
    config.Client.Database,
    config.Client.Timeout,
    config.Client.Tries,
    config.Client.Sleep
  )
  
  /**
   * Constructs a $client instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Client(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $client
   */
  def this(config: Config) = this(RedisConfig(config))
  
  /**
   * Constructs a $client instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def this(config: Config, path: String) = this(RedisConfig(config, path))
  
  /**
   * Constructs a $client instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Client(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $client
   */
  def this(configName: String) = this(RedisConfig(configName))
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def this(configName: String, path: String) = this(RedisConfig(configName, path))
  
  protected[scredis] val DefaultCommandOptions = CommandOptions(timeout, tries, sleep)
  
  protected val connection = new JavaSocketConnection(
    host,
    port,
    timeout,
    Some(authAndSelect)
  )
  
  private def authAndSelect(): Unit = {
    password.map(auth)
    if (database > 0) select(database)
  }

  def currentTimeout: Duration = connection.currentTimeout
  def setTimeout(timeout: Duration): Unit = connection.setTimeout(timeout)
  def restoreDefaultTimeout(): Unit = connection.restoreDefaultTimeout()

  def isConnected: Boolean = connection.isConnected
  
  /**
   * Authenticates to the server.
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  override def auth(
    password: String
  )(implicit opts: CommandOptions = DefaultCommandOptions): Unit = {
    this.password = Some(password)
    super.auth(password)
  }
  
  /**
   * Changes the selected database on the current client.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  override def select(db: Int)(implicit opts: CommandOptions = DefaultCommandOptions): Unit = {
    database = db
    super.select(db)
  }

  try {
    connection.connect()
  } catch {
    case e: Throwable =>
  }
  
}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define client [[scredis.Client]]
 * @define tc com.typesafe.Config
 */
object Client {

  /**
   * Creates a $client
   * 
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
    host: String = RedisConfigDefaults.Client.Host,
    port: Int = RedisConfigDefaults.Client.Port,
    password: Option[String] = RedisConfigDefaults.Client.Password,
    database: Int = RedisConfigDefaults.Client.Database,
    timeout: Duration = RedisConfigDefaults.Client.Timeout,
    tries: Int = RedisConfigDefaults.Client.Tries,
    sleep: Option[FiniteDuration] = RedisConfigDefaults.Client.Sleep
  ): Client = new Client(host, port, password, database, timeout, tries, sleep)
  
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def apply(config: RedisConfig): Client = new Client(config)
  
  /**
   * Constructs a $client instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Client(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $client
   */
  def apply(config: Config): Client = new Client(config)
  
  /**
   * Constructs a $client instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(config: Config, path: String): Client = new Client(config, path)
  
  /**
   * Constructs a $client instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Client(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $client
   */
  def apply(configName: String): Client = new Client(configName)
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(configName: String, path: String): Client = new Client(configName, path)

}