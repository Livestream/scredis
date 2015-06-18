package scredis.io

import akka.actor.ActorSystem
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ClusterRequests.{ClusterInfo, ClusterCountKeysInSlot, ClusterSlots}
import scredis.util.UniqueNameGenerator
import scredis.{ClusterSlotRange, RedisConfigDefaults, Server}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Promise, Await, Future}
import scala.concurrent.duration._

/**
 * The connection logic for a whole Redis cluster. Handles redirection and sharding logic as specified in
 * http://redis.io/topics/cluster-spec
 *
 * @param nodes List of servers to initialize the cluster connection with.
 * @param maxRetries Maximum number of retries or redirects for a request. The ClusterConnection may attempt to try
 *                   several nodes before giving up on sending a command.
 */
abstract class ClusterConnection(
    nodes: Seq[Server], maxRetries: Int = 4,
    receiveTimeoutOpt: Option[FiniteDuration] = RedisConfigDefaults.IO.ReceiveTimeoutOpt,
    connectTimeout: FiniteDuration = RedisConfigDefaults.IO.ConnectTimeout,
    maxWriteBatchSize: Int = RedisConfigDefaults.IO.MaxWriteBatchSize,
    tcpSendBufferSizeHint: Int = RedisConfigDefaults.IO.TCPSendBufferSizeHint,
    tcpReceiveBufferSizeHint: Int = RedisConfigDefaults.IO.TCPReceiveBufferSizeHint,
    akkaListenerDispatcherPath: String = RedisConfigDefaults.IO.Akka.ListenerDispatcherPath,
    akkaIODispatcherPath: String = RedisConfigDefaults.IO.Akka.IODispatcherPath,
    akkaDecoderDispatcherPath: String = RedisConfigDefaults.IO.Akka.DecoderDispatcherPath
  ) extends NonBlockingConnection {

  // TODO determine good values for these wait durations, make configurable?
  /** How long to wait after tryAgain error before retrying a send. */
  private val tryAgainWait = 10.millis
  /** How long to wait after a CLUSTERDOWN message before retrying a send. */
  private val clusterDownWait = 100.millis

  private val scheduler = ActorSystem().scheduler

  /** Set of active cluster node connections. */
  // TODO it may be more efficient to save the connections in hashSlots directly
  // TODO we need some information about node health for updates
  private var connections: Map[Server, NonBlockingConnection] =
    nodes.map { server => (server, makeConnection(server))}.toMap


  // TODO keep master-slave associations to be able to ask slaves for data with appropriate config

  /** hash slot - connection mapping */
  // TODO we can probably use a more efficient storage here, but this is simplest
  private var hashSlots: Vector[Option[Server]] = Vector.fill(Protocol.CLUSTER_HASHSLOTS)(None)

  // bootstrapping: init with info from cluster
  // TODO is it okay to to a blocking await here? or move it to a factory method?
  Await.ready(updateCache(maxRetries), connectTimeout)

  /**
   * Update the ClusterClient's connections and hash slots cache.
   * Sends a CLUSTER SLOTS query to the cluster to get a current mapping of hash slots to servers, and updates
   * the internal cache based on the reply.
   */
  private def updateCache(retry:Int): Future[Unit] = {

    // only called when cluster is in ok state
    lazy val upd = send(ClusterSlots()).map { slotRanges =>
      val newConnections = slotRanges.foldLeft(connections) {
        case (cons, ClusterSlotRange(_, master, _)) =>
          if (cons contains master) cons
          else connections + ((master, makeConnection(master)))
      }

      val newSlots =
        slotRanges.foldLeft(hashSlots) {
          case (slots, ClusterSlotRange((begin, end), master, replicas)) =>
            val masterOpt = Option(master)
            (begin.toInt to end.toInt).foldLeft(slots) { (s, i) => s.updated(i, masterOpt) }
        }
      this.synchronized {
        connections = newConnections
        hashSlots = newSlots
      }
    }

    if (retry < 0) Future.failed(RedisClusterException("cluster_state not ok, aborting cache update"))
    // ensure availability of cluster first
    else send(ClusterInfo()).flatMap { info =>
      info.get("cluster_state") match {
        case Some("ok") => upd
        case _ => delayed(clusterDownWait) { updateCache(retry - 1) }
      }
    }
  }

  /** Creates a new connection to a server. */
  private def makeConnection(server: Server): NonBlockingConnection = {
    val systemName = RedisConfigDefaults.IO.Akka.ActorSystemName
    val system = ActorSystem(UniqueNameGenerator.getUniqueName(systemName))

    new AkkaNonBlockingConnection(
      system = system, host = server.host, port = server.port, passwordOpt = None,
      database = 0, nameOpt = None, decodersCount = 2,
      receiveTimeoutOpt, connectTimeout, maxWriteBatchSize, tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint, akkaListenerDispatcherPath, akkaIODispatcherPath,
      akkaDecoderDispatcherPath
    ) {}
  }

  /** Delay a Future-returning operation. */
  private def delayed[A](delay: Duration)(f: => Future[A]): Future[A] = {
    val delayedF = Promise[A]
    scheduler.scheduleOnce(clusterDownWait) {
      delayedF.tryCompleteWith(f)
    }
    delayedF.future
  }

  /**
   * Send a Redis request object and handle cluster specific error cases
   * @param request request object
   * @param server server to contact
   * @param retry remaining retries
   * @tparam A response type
   * @return
   */
  private def send[A](request: Request[A], server: Server, retry: Int): Future[A] = {
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
          request.reset()
          // TODO this will usually happen when cache is missed. Keep track of misses and reinitialize fully eventually
          val movedServer: Server = Server(host, port)
          // I believe it is safe to synchronize only updates to hashSlots.
          // If a read is outdated it will be redirected anyway and potentially repeat the update.
          this.synchronized { hashSlots = hashSlots.updated(slot, Option(movedServer)) }
          send(request, movedServer, retry - 1)

        case RedisClusterErrorResponseException(Ask(hashSlot, host, port), _) =>
          request.reset()
          val askServer = Server(host,port)
          send(request, askServer, retry - 1)

        case RedisClusterErrorResponseException(TryAgain, _) =>
          request.reset()
          // TODO what is actually the intended semantics of TryAgain?
          delayed(tryAgainWait) { send(request, server, retry - 1) }

        case RedisClusterErrorResponseException(ClusterDown, _) =>
          request.reset()
          // wait a bit because the cluster may not be fully initialized or fixing itself
          delayed(clusterDownWait) { send(request, server, retry - 1) }

        case ex @ RedisIOException(message, cause) =>
          request.reset()
          // TODO we should keep track of server failures to decide when to evict a node from the cache
          // try any server that isn't the one we tried already
          connections.keys.find { _ != server } match {
            case Some(nextServer) => send(request, nextServer, retry - 1)
            case None => Future.failed(RedisIOException("No valid connection available.", ex))
          }
      }
    }
  }

  override protected[scredis] def send[A](request: Request[A]): Future[A] =
    request match {

      case req @ ClusterCountKeysInSlot(slot) =>
        // special case handling for slot counting in clusters: redirect to the proper cluster node
        if (slot >= Protocol.CLUSTER_HASHSLOTS || slot < 0)
          Future.failed(RedisInvalidArgumentException(s"Invalid slot number: $slot"))
        else hashSlots(slot.toInt) match {
          case None => Future.failed(RedisIOException(s"No cluster slot information available for $slot"))
          case Some(server) => send(req, server, maxRetries)
        }

      case keyReq: Request[A] with Key =>
        // TODO when we can't get a cached server, just get the first connection
        hashSlots(ClusterCRC16.getSlot(keyReq.key)) match {
          case None =>
            if (connections.isEmpty) Future.failed(RedisIOException("No cluster node connection available"))
            else send(keyReq, connections.head._1, maxRetries)
          case Some(server) =>
            send(keyReq, server, maxRetries)
        }

      case clusterReq: Request[A] with Cluster =>
        // requests that are valid with any cluster node
        val server = connections.head._1
        send(clusterReq, server, maxRetries)
      case _ =>
        // TODO what to do about non-key requests? they would be valid for any individual cluster node, but arbitrary choice is probably not what's intended..
        Future.failed(RedisInvalidArgumentException("This command is not supported for clusters"))
    }

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}
