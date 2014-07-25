package scredis

import com.typesafe.config.Config

import akka.actor._
import akka.routing._

import scredis.io.AkkaNonBlockingConnection
import scredis.commands._
import scredis.util.UniqueNameGenerator

import scala.util.{ Success, Failure }
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define redis [[scredis.Redis]]
 * @define tc com.typesafe.Config
 */
final class Redis(protected val config: RedisConfig) extends AkkaNonBlockingConnection(
  system = ActorSystem(UniqueNameGenerator.getUniqueName(config.IO.Akka.ActorSystemName)),
  host = config.Redis.Host,
  port = config.Redis.Port,
  passwordOpt = config.Redis.PasswordOpt,
  database = config.Redis.Database,
  nameOpt = config.Redis.NameOpt,
  connectTimeout = config.IO.ConnectTimeout,
  receiveTimeoutOpt = config.IO.ReceiveTimeoutOpt,
  maxWriteBatchSize = config.IO.MaxWriteBatchSize,
  tcpSendBufferSizeHint = config.IO.TCPSendBufferSizeHint,
  tcpReceiveBufferSizeHint = config.IO.TCPReceiveBufferSizeHint,
  akkaListenerDispatcherPath = config.IO.Akka.ListenerDispatcherPath,
  akkaIODispatcherPath = config.IO.Akka.IODispatcherPath,
  akkaDecoderDispatcherPath = config.IO.Akka.DecoderDispatcherPath,
  decodersCount = 2
) with ConnectionCommands
  with ServerCommands
  with KeyCommands
  with StringCommands
  with HashCommands
  with ListCommands
  with SetCommands
  with SortedSetCommands
  with ScriptingCommands
  with HyperLogLogCommands
  with PubSubCommands
  with TransactionCommands {
  
  private var shouldShutdownBlockingClient = false
  private var shouldShutdownSubscriberClient = false
  
  /**
   * 
   */
  lazy val blocking = {
    shouldShutdownBlockingClient = true
    BlockingClient(config)(system)
  }
  
  /**
   * 
   */
  lazy val subscriber = {
    shouldShutdownSubscriberClient = true
    SubscriberClient(config)(system)
  }
  
  /**
   * Constructs a $redis instance using the default config
   * 
   * @return the constructed $redis
   */
  def this() = this(RedisConfig())
  
  /**
   * Constructs a $redis instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def this(config: Config) = this(RedisConfig(config))
  
  /**
   * Constructs a $redis instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Redis(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $redis
   */
  def this(configName: String) = this(RedisConfig(configName))
  
  /**
   * Constructs a $redis instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def this(configName: String, path: String) = this(RedisConfig(configName, path))
  
  /**
   * Authenticates to the server.
   * 
   * @note use the empty string to re-authenticate with no password
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  override def auth(password: String): Future[Unit] = {
    if (shouldShutdownBlockingClient) {
      try {
        blocking.auth(password)(5 seconds)
      } catch {
        case e: Throwable => logger.error("Could not authenticate blocking client", e)
      }
    }
    if (shouldShutdownSubscriberClient) {
      subscriber.auth(password)
    } else {
      Future.successful(())
    }.recover {
      case e: Throwable => logger.error("Could not authenticate subscriber client", e)
    }.flatMap { _ =>
      super.auth(password)
    }
  }

  /**
   * Closes the connection.
   *
   * @since 1.0.0
   */
  override def quit(): Future[Unit] = {
    if (shouldShutdownBlockingClient) {
      try {
        blocking.quit()(5 seconds)
      } catch {
        case e: Throwable => logger.error("Could not shutdown blocking client", e)
      }
    }
    if (shouldShutdownSubscriberClient) {
      subscriber.quit()
    } else {
      Future.successful(())
    }.recover {
      case e: Throwable => logger.error("Could not shutdown subscriber client", e)
    }.flatMap { _ =>
      super.quit()
    }.map { _ =>
      system.shutdown()
    }
  }

  /**
   * Changes the selected database on the current connection.
   *
   * @param database database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  override def select(database: Int): Future[Unit] = {
    val future = if (shouldShutdownBlockingClient) {
      try {
        Future.successful(blocking.select(database)(5 seconds))
      } catch {
        case e: Throwable => Future.failed(e)
      }
    } else {
      Future.successful(())
    }
    future.flatMap { _ =>
      super.select(database)
    }
  }
  
}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define redis [[scredis.Redis]]
 * @define tc com.typesafe.Config
 */
object Redis {
  
  /**
   * Constructs a $redis instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $redis
   */
  def apply(config: RedisConfig = RedisConfig()): Redis = new Redis(config)
  
  /**
   * Constructs a $redis instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def apply(config: Config): Redis = new Redis(config)
  
  /**
   * Constructs a $redis instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Redis(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $redis
   */
  def apply(configName: String): Redis = new Redis(configName)
  
  /**
   * Constructs a $redis instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def apply(configName: String, path: String): Redis = new Redis(configName, path)
  
}
