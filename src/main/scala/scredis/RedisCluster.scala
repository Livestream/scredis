package scredis

import com.typesafe.config.Config
import scredis.commands._
import scredis.io.{ClusterConnection, Connection}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Defines a `RedisCluster` [[scredis.Client]] supporting all non-blocking commands that can be addressed to either
  * any cluster node or be automatically routed to the correct node.
  *
  * @define e [[scredis.exceptions.RedisErrorResponseException]]
  * @define redisCluster [[scredis.RedisCluster]]
  * @define typesafeConfig com.typesafe.Config
  */
private[scredis] class RedisCluster(
    nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
    maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
    tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
    clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait
  )
  extends ClusterConnection(
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait
  ) with Connection
  with ClusterCommands
  with HashCommands
  with HyperLogLogCommands
  with KeyCommands
  with ListCommands
  with PubSubCommands
  with ScriptingCommands
  with SetCommands
  with SortedSetCommands
  with StringCommands
  //with SubscriberCommands
{
  override implicit val dispatcher: ExecutionContext =
    ExecutionContext.Implicits.global // TODO perhaps implement our own

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @return the constructed $redisCluster
    */
  def this(config: RedisConfig) = this(
    nodes = config.Redis.ClusterNodes,
    maxRetries = 4,
    receiveTimeoutOpt = config.IO.ReceiveTimeoutOpt,
    connectTimeout = config.IO.ConnectTimeout,
    maxWriteBatchSize = config.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint = config.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint = config.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath = config.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath = config.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath = config.IO.Akka.DecoderDispatcherPath,
    tryAgainWait = config.IO.Cluster.TryAgainWait,
    clusterDownWait = config.IO.Cluster.ClusterDownWait
  )

  /**
    * Constructs a $redisCluster instance using the default config.
    *
    * @return the constructed $redisCluster
    */
  def this() = this(RedisConfig())

}


object RedisCluster {

  /**
    * Constructs a $redisCluster instance using provided parameters.
    *
    * @param nodes list of server nodes, used as seed nodes to initially connect to the cluster.
    * @param maxRetries maximum number of retries and redirects to perform for a single command
    * @param receiveTimeoutOpt optional batch receive timeout
    * @param connectTimeout connection timeout
    * @param maxWriteBatchSize max number of bytes to send as part of a batch
    * @param tcpSendBufferSizeHint size hint of the tcp send buffer, in bytes
    * @param tcpReceiveBufferSizeHint size hint of the tcp receive buffer, in bytes
    * @param akkaListenerDispatcherPath path to listener dispatcher definition
    * @param akkaIODispatcherPath path to io dispatcher definition
    * @param akkaDecoderDispatcherPath path to decoder dispatcher definition
    * @param tryAgainWait time to wait after a TRYAGAIN response by a cluster node
    * @param clusterDownWait time to wait for a retry after CLUSTERDOWN response
    * @return the constructed $redisCluster
    */
  def apply(
    nodes: Seq[Server] = RedisConfigDefaults.Redis.ClusterNodes,
    maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath,
    tryAgainWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.TryAgainWait,
    clusterDownWait: FiniteDuration = RedisConfigDefaults.IO.Cluster.ClusterDownWait
  ) = new RedisCluster(
    nodes = nodes,
    maxRetries = maxRetries,
    receiveTimeoutOpt = receiveTimeoutOpt,
    connectTimeout = connectTimeout,
    maxWriteBatchSize = maxWriteBatchSize,
    tcpSendBufferSizeHint = tcpReceiveBufferSizeHint,
    akkaListenerDispatcherPath = akkaListenerDispatcherPath,
    akkaIODispatcherPath = akkaIODispatcherPath,
    tryAgainWait = tryAgainWait,
    clusterDownWait = clusterDownWait
  )


  /**
    * Constructs a $redisCluster instance with given seed nodes, using the default config for all other parameters.
    *
    * @return the constructed $redisCluster
    */
  def apply(node: Server, nodes: Server*): RedisCluster = RedisCluster( nodes = node +: nodes)

  /**
    * Constructs a $redisCluster instance from a [[scredis.RedisConfig]].
    *
    * @param config a [[scredis.RedisConfig]]
    * @return the constructed $redisCluster
    */
  def apply(config: RedisConfig) = new RedisCluster(config)

  /**
    * Constructs a $redisCluster instance from a $tc.
    *
    * @note The config must contain the scredis object at its root.
    *
    * @param config a $typesafeConfig
    * @return the constructed $redis
    */
  def apply(config: Config) = new RedisCluster(RedisConfig(config))


  /**
    * Constructs a $redisCluster instance from a config file.
    *
    * @note The config file must contain the scredis object at its root.
    * This constructor is equivalent to {{{
    * Redis(configName, "scredis")
    * }}}
    *
    * @param configName config filename
    * @return the constructed $redis
    */
  def apply(configName: String): RedisCluster = new RedisCluster(RedisConfig(configName))

  /**
    * Constructs a $redisCluster instance from a config file and using the provided path.
    *
    * @note The path must include to the scredis object, e.g. x.y.scredis
    *
    * @param configName config filename
    * @param path path pointing to the scredis config object
    * @return the constructed $redis
    */
  def apply(configName: String, path: String): RedisCluster = new RedisCluster(RedisConfig(configName, path))

}