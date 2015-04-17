package scredis.io

import akka.actor.ActorSystem
import scredis.RedisConfigDefaults
import scredis.exceptions._
import scredis.protocol._
import scredis.util.UniqueNameGenerator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// TODO move this
case class Server(host: String, port: Int)

/**
 * The connection logic for a whole Redis cluster. Handles redirection and sharding logic as specified in
 * http://redis.io/topics/cluster-spec
 *
 * @param nodes List of servers to initialize the cluster connection with.
 */
abstract class ClusterConnection(val nodes: List[Server]) extends NonBlockingConnection {

  /** hash slot - connection mapping */
  private var hashSlots: Vector[Option[Server]] = Vector.fill(Protocol.CLUSTER_HASHSLOTS)(None)

  /** Set of active cluster node connections. */
  // TODO it may be more efficient to save the connections in hashSlots directly
  private var connections: Map[Server, NonBlockingConnection] =
    nodes.map { server => (server, makeConnection(server))}.toMap

  /** Creates a new connection to a server. */
  private def makeConnection(server: Server): NonBlockingConnection = {
    val systemName = RedisConfigDefaults.IO.Akka.ActorSystemName
    val system = ActorSystem(UniqueNameGenerator.getUniqueName(systemName))

    new AkkaNonBlockingConnection(
      // TODO get these as parameters
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
  private def hashSlot(key: String): Int = ClusterCRC16.getSlot(key) // FIXME dummy implementation


  private def send[A](request: Request[A], server: Server, retry: Int): Future[A] = {
    // TODO better retry information, perhaps redirect limit too?
    if (retry <= 0) Future.failed(RedisIOException(s"Gave up on request after several retries: $request"))
    else {
      val connection = connections.getOrElse(server, {
        val con = makeConnection(server)
        connections = this.synchronized { connections.updated(server, con) }
        con
      })

      // handle cases that should be retried (MOVE, ASK, TRYAGAIN)
      connection.send(request).recoverWith {

        case RedisClusterErrorResponseException(Moved(slot, host, port), _) =>
          val movedServer: Server = Server(host, port)
          // I believe it is safe to synchronize only updates to hashSlots.
          // If a read is outdated it will be redirected anyway and potentially repeat the update.
          this.synchronized { hashSlots = hashSlots.updated(slot, Option(movedServer)) }
          request.reset
          send(request, movedServer, retry - 1)

        case RedisClusterErrorResponseException(Ask(hashSlot, host, port), _) =>
          val askServer = Server(host,port)
          request.reset
          send(request, askServer, retry - 1)

        case RedisClusterErrorResponseException(TryAgain, _) =>
          // TODO wait a bit? what's the intended semantics?
          request.reset
          send(request, server, retry - 1)

        // TODO handle timeout etc: try a different server
        // TODO handle CLUSTERDOWN?
      }
    }
  }

  override protected[scredis] def send[A](request: Request[A]): Future[A] =
    request match {
      case keyReq: Request[A] with Key =>
        // TODO perhaps choose a random server instead?
        val server = hashSlots(hashSlot(keyReq.key)).getOrElse(connections.head._1)
        send(keyReq, server, 3)
      case _ =>
        // TODO what to do about non-key requests? they would be valid for any individual cluster node
        Future.failed(RedisInvalidArgumentException("This command is not supported for clusters"))
    }

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}
