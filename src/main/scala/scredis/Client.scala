package scredis

import com.typesafe.config.Config

import akka.actor.ActorSystem

import scredis.io.AkkaNonBlockingConnection
import scredis.protocol.Protocol
import scredis.commands._
import scredis.exceptions._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Defines a Redis client supporting all non-blocking commands.
 * 
 * @param host server address
 * @param port server port
 * @param passwordOpt optional server password
 * @param database database index to select
 * @param nameOpt optional client name (available since 2.6.9)
 * @param connectTimeout connection timeout
 * @param receiveTimeoutOpt optional batch receive timeout
 * @param maxWriteBatchSize max number of bytes to send as part of a batch
 * @param tcpSendBufferSizeHint size hint of the tcp send buffer, in bytes
 * @param tcpReceiveBufferSizeHint size hint of the tcp receive buffer, in bytes
 * @param akkaListenerDispatcherPath path to listener dispatcher definition
 * @param akkaIODispatcherPath path to io dispatcher definition
 * @param akkaDecoderDispatcherPath path to decoder dispatcher definition
 * 
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define client [[scredis.Client]]
 * @define tc com.typesafe.Config
 */
final class Client(
  host: String = RedisConfigDefaults.Redis.Host,
  port: Int = RedisConfigDefaults.Redis.Port,
  passwordOpt: Option[String] = RedisConfigDefaults.Redis.PasswordOpt,
  database: Int = RedisConfigDefaults.Redis.Database,
  nameOpt: Option[String] = RedisConfigDefaults.Redis.NameOpt,
  connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
  receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
  maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
  tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
  tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
  akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
  akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
  akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath
)(implicit system: ActorSystem) extends AkkaNonBlockingConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = database,
  nameOpt = nameOpt,
  connectTimeout = connectTimeout,
  receiveTimeoutOpt = receiveTimeoutOpt,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  decodersCount = 2,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
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
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def this(config: RedisConfig)(implicit system: ActorSystem) = this(
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
    akkaDecoderDispatcherPath = config.IO.Akka.DecoderDispatcherPath
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
   * @param passwordOpt optional server password
   * @param database database index to select
   * @param nameOpt optional client name (available since 2.6.9)
   * @param connectTimeout connection timeout
   * @param receiveTimeoutOpt optional batch receive timeout
   * @param maxWriteBatchSize max number of bytes to send as part of a batch
   * @param tcpSendBufferSizeHint size hint of the tcp send buffer, in bytes
   * @param tcpReceiveBufferSizeHint size hint of the tcp receive buffer, in bytes
   * @param akkaListenerDispatcherPath path to listener dispatcher definition
   * @param akkaIODispatcherPath path to io dispatcher definition
   * @param akkaDecoderDispatcherPath path to decoder dispatcher definition
   */
  def apply(
    host: String = RedisConfigDefaults.Redis.Host,
    port: Int = RedisConfigDefaults.Redis.Port,
    passwordOpt: Option[String] = RedisConfigDefaults.Redis.PasswordOpt,
    database: Int = RedisConfigDefaults.Redis.Database,
    nameOpt: Option[String] = RedisConfigDefaults.Redis.NameOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath
  )(implicit system: ActorSystem): Client = new Client(
    host = host,
    port = port,
    passwordOpt = passwordOpt,
    database = database,
    nameOpt = nameOpt,
    connectTimeout = connectTimeout,
    receiveTimeoutOpt = receiveTimeoutOpt,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpSendBufferSizeHint,
    tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
  )
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def apply(config: RedisConfig)(implicit system: ActorSystem): Client = new Client(config)
  
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
  def apply(config: Config)(implicit system: ActorSystem): Client = new Client(config)
  
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
  def apply(configName: String)(implicit system: ActorSystem): Client = new Client(configName)
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(configName: String, path: String)(implicit system: ActorSystem): Client = new Client(
    configName, path
  )

}