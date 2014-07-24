package scredis

import com.typesafe.config.Config

import akka.actor.ActorSystem

import scredis.io.AkkaBlockingConnection
import scredis.protocol.Protocol
import scredis.protocol.requests.ConnectionRequests.{ Auth, Select, Quit }
import scredis.commands._
import scredis.exceptions._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Defines a Redis client supporting all commands.
 * 
 * @param host server address
 * @param port server port
 * @param password server password
 * @param database database index to select
 * @param timeout maximum duration for the execution of a command, can be infinite
 * 
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define client [[scredis.BlockingClient]]
 * @define tc com.typesafe.Config
 */
final class BlockingClient(
  host: String = RedisConfigDefaults.Client.Host,
  port: Int = RedisConfigDefaults.Client.Port,
  passwordOpt: Option[String] = RedisConfigDefaults.Client.Password,
  database: Int = RedisConfigDefaults.Client.Database,
  timeout: Duration = RedisConfigDefaults.Client.Timeout
)(implicit system: ActorSystem) extends AkkaBlockingConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = database
) with BlockingListCommands {
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def this(config: RedisConfig)(implicit system: ActorSystem) = this(
    config.Client.Host,
    config.Client.Port,
    config.Client.Password,
    config.Client.Database,
    config.Client.Timeout
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
  def this(config: Config)(implicit system: ActorSystem) = this(RedisConfig(config))
  
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
  def this(configName: String)(implicit system: ActorSystem) = this(RedisConfig(configName))
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def this(configName: String, path: String)(implicit system: ActorSystem) = this(
    RedisConfig(configName, path)
  )
  
  /**
   * Authenticates to the server.
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String)(implicit timeout: Duration): Unit = sendBlocking(Auth(password))
  
  /**
   * Changes the selected database on the current client.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(database: Int)(implicit timeout: Duration): Unit = sendBlocking(Select(database))
  
  /**
   * Closes the connection.
   *
   * @since 1.0.0
   */
  def quit()(implicit timeout: Duration): Unit = sendBlocking(Quit())
  
}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define client [[scredis.BlockingClient]]
 * @define tc com.typesafe.Config
 */
object BlockingClient {
  
  /**
   * Creates a $client
   * 
   * @param host server address
   * @param port server port
   * @param password server password
   * @param database database index to select
   * @param timeout maximum duration for the execution of a command, can be infinite
   */
  def apply(
    host: String = RedisConfigDefaults.Client.Host,
    port: Int = RedisConfigDefaults.Client.Port,
    passwordOpt: Option[String] = RedisConfigDefaults.Client.Password,
    database: Int = RedisConfigDefaults.Client.Database,
    timeout: Duration = RedisConfigDefaults.Client.Timeout
  )(implicit system: ActorSystem): BlockingClient = new BlockingClient(
    host, port, passwordOpt, database, timeout
  )
  
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def apply(config: RedisConfig)(
    implicit system: ActorSystem
  ): BlockingClient = new BlockingClient(config)
  
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
  def apply(config: Config)(
    implicit system: ActorSystem
  ): BlockingClient = new BlockingClient(config)
  
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
  def apply(configName: String)(
    implicit system: ActorSystem
  ): BlockingClient = new BlockingClient(configName)
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(configName: String, path: String)(
    implicit system: ActorSystem
  ): BlockingClient = new BlockingClient(
    configName, path
  )

}