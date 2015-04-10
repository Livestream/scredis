package scredis.io

import scredis.protocol.{Key, Request}

import scala.concurrent.Future

/**
 * Implements the connection to a whole cluster
 */
//protected val system: ActorSystem,
//val host: String,
//val port: Int,
//@volatile protected var passwordOpt: Option[String],
//@volatile protected var database: Int,
//@volatile protected var nameOpt: Option[String],
//protected val decodersCount: Int,
//protected val receiveTimeoutOpt: Option[FiniteDuration],
//protected val connectTimeout: FiniteDuration,
//protected val maxWriteBatchSize: Int,
//protected val tcpSendBufferSizeHint: Int,
//protected val tcpReceiveBufferSizeHint: Int,
//protected val akkaListenerDispatcherPath: String,
//protected val akkaIODispatcherPath: String,
//protected val akkaDecoderDispatcherPath: String


//class Redis private[scredis] (
//   systemOrName: Either[ActorSystem, String],
//   host: String,
//   port: Int,
//   passwordOpt: Option[String],
//   database: Int,
//   nameOpt: Option[String],
//   connectTimeout: FiniteDuration,
//   receiveTimeoutOpt: Option[FiniteDuration],
//   maxWriteBatchSize: Int,
//   tcpSendBufferSizeHint: Int,
//   tcpReceiveBufferSizeHint: Int,
//   akkaListenerDispatcherPath: String,
//   akkaIODispatcherPath: String,
//   akkaDecoderDispatcherPath: String
//   )

class Server(host: String, port: Int, passwordOpt: Option[String])

abstract class ClusterConnection(val nodes: List[Server]) extends NonBlockingConnection {

  // TODO evaluate thread safety
  // hypothesis: there is no correctness issue with multiple threads updating the slot cache,
  // but there may be redundant work and inefficiencies


  /** hash slot - connection mapping */
  private var hashSlots = ???

  /** Set of active cluster node connections. */
  private var connections: List[NonBlockingConnection] = ???

  /** Best guess for a connection, given a key. */
  private def connection(key: String): NonBlockingConnection = ???

  /** Calculate hash slot for a key.
    *
    * Algorithm specification: http://redis.io/topics/cluster-spec#keys-hash-tags
    */
  private def hashSlot(key: String): Int = ???

  override protected[scredis] def send[A](request: Request[A]): Future[A] =
    request match {
      case keyReq: Key => connection(keyReq.key).send(request)
      case _ => Future.failed(new Exception("This command is not supported for clusters")) // TODO what to do about non-key requests?
    }

  // handle responses where?
  // send stuff via connection
  // for cluster-specific error responses, handle appropriately

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}
