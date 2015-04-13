package scredis.io

import akka.actor.ActorSystem
import scredis.RedisConfigDefaults
import scredis.exceptions._
import scredis.protocol._
import scredis.util.UniqueNameGenerator

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

case class Server(host: String, port: Int, passwordOpt: Option[String])

/**
 * The connection logic for a whole cluster.
 */
class ClusterConnection(val nodes: List[Server]) extends NonBlockingConnection {

  // TODO evaluate thread safety
  // hypothesis: there is no correctness issue with multiple threads updating the slot cache,
  // but there may be redundant work and inefficiencies

//  class NodeConnection extends AkkaNonBlockingConnection {
//
//  }


  /** hash slot - connection mapping */
  private var hashSlots: Vector[Option[Server]] = Vector.fill(16384)(None)

  /** Set of active cluster node connections. */
  private var connections: Map[Server, NonBlockingConnection] =
    nodes.map { server => (server, makeConnection(server))}.toMap

  /** Creates a new connection to a server. */
  private def makeConnection(server: Server): NonBlockingConnection = {
    val systemName = RedisConfigDefaults.IO.Akka.ActorSystemName
    val system = ActorSystem(UniqueNameGenerator.getUniqueName(systemName))

    new AkkaNonBlockingConnection(
      system = system, host = server.host, port = server.port, passwordOpt = None,
      database = 0, nameOpt = None, decodersCount = 2,
      receiveTimeoutOpt = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
      connectTimeout = RedisConfigDefaults.IO.ConnectTimeout,
      maxWriteBatchSize = RedisConfigDefaults.IO.MaxWriteBatchSize,
      tcpSendBufferSizeHint = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
      tcpReceiveBufferSizeHint = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
      akkaListenerDispatcherPath = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
      akkaIODispatcherPath = RedisConfigDefaults.IO.Akka.IODispatcherPath,
      akkaDecoderDispatcherPath = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath
    ) {}
  }

  /** Calculate hash slot for a key.
    *
    * Algorithm specification: http://redis.io/topics/cluster-spec#keys-hash-tags
    */
  private def hashSlot(key: String): Int = 0 // FIXME dummy implementation


  private def send[A](request: Request[A] with Key, server: Server, retry: Int): Future[A] = {
    // TODO better retry information, perhaps redirect limit too?
    if (retry <= 0) Future.failed(RedisIOException(s"Gave up on request after several retries: $request"))
    else {
      val connection = connections.getOrElse(server, {
        val con = makeConnection(server)
        connections = connections.updated(server, con)
        con
      })
      connection.send(request).recoverWith {

        case RedisClusterErrorResponseException(Moved(slot, host, port), _) =>
          val server: Server = Server(host, port, None)
          hashSlots = hashSlots.updated(slot, Option(server))
          send(request, server, retry)
        case RedisClusterErrorResponseException(Ask(hashSlot, host, port), _) =>
          val server = Server(host,port,None)
          send(request, server, retry)
        case RedisClusterErrorResponseException(TryAgain, _) =>
          // TODO wait a bit? limited retry semantics?
          send(request, server, retry - 1)

          // TODO other error cases?
      }
    }
  }

  override protected[scredis] def send[A](request: Request[A]): Future[A] =
    request match {
      case keyReq: Request[A] with Key =>
        val server = hashSlots(hashSlot(keyReq.key)).getOrElse(connections.head._1)
        send(keyReq, server, 3)
      case _ =>
        // TODO what to do about non-key requests? they would be valid for any individual cluster node
        Future.failed(RedisInvalidArgumentException("This command is not supported for clusters"))
    }

  // handle responses where?
  // send stuff via connection
  // for cluster-specific error responses, handle appropriately

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}
