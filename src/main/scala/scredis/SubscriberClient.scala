package scredis

import com.typesafe.config.Config

import akka.actor.ActorSystem

import scredis.io.SubscriberAkkaConnection
import scredis.protocol.Protocol
import scredis.commands._
import scredis.exceptions._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Defines a Pub/Sub Redis client capable of subscribing to channels/patterns.
 * 
 * @param host server address
 * @param port server port
 * @param password server password
 * @param database database index to select
 * @param timeout maximum duration for the execution of a command, can be infinite
 * 
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define client [[scredis.SubscriberClient]]
 * @define tc com.typesafe.Config
 */
final class SubscriberClient(
  host: String = RedisConfigDefaults.Client.Host,
  port: Int = RedisConfigDefaults.Client.Port,
  passwordOpt: Option[String] = RedisConfigDefaults.Client.Password,
  timeout: Duration = RedisConfigDefaults.Client.Timeout
)(implicit system: ActorSystem) extends SubscriberAkkaConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = 0
) with SubscriberCommands {
  
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
   * @note use the empty string to re-authenticate with no password
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String)(implicit timeout: Duration): Unit = authenticate(password)
  
  /**
   * Unsubscribes from all subscribed channels/patterns and then closes the connection.
   */
  def quit()(implicit timeout: Duration): Unit = shutdown()
  
}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define client [[scredis.SubscriberClient]]
 * @define tc com.typesafe.Config
 */
object SubscriberClient {

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
    timeout: Duration = RedisConfigDefaults.Client.Timeout
  )(implicit system: ActorSystem): SubscriberClient = new SubscriberClient(
    host, port, passwordOpt, timeout
  )
  
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def apply(config: RedisConfig)(
    implicit system: ActorSystem
  ): SubscriberClient = new SubscriberClient(config)
  
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
  ): SubscriberClient = new SubscriberClient(config)
  
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
  ): SubscriberClient = new SubscriberClient(configName)
  
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
  ): SubscriberClient = new SubscriberClient(configName, path)
  
}