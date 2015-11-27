package scredis.io

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
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
    nodes: Seq[Server],
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
  ) extends NonBlockingConnection with LazyLogging {

  private val maxHashMisses = 100
  private val maxConnectionMisses = 3

  private val scheduler = ActorSystem().scheduler

  /** Set of active cluster node connections. */
  // TODO it may be more efficient to save the connections in hashSlots directly
  // TODO we need some information about node health for updates
  private var connections: Map[Server, (NonBlockingConnection, Int)] =
    nodes.map { server => (server, (makeConnection(server), 0))}.toMap

  // TODO keep master-slave associations to be able to ask slaves for data with appropriate config

  /** hash slot - connection mapping */
  // TODO we can probably use a more space-efficient storage here, but this is simplest
  private var hashSlots: Vector[Option[Server]] = Vector.fill(Protocol.CLUSTER_HASHSLOTS)(None)

  /** Miss counter for hashSlots accesses. */
  private var hashMisses = 0

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
          else connections + ((master, (makeConnection(master),0)))
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

      logger.info("initialized cluster slot cache")
    }

    if (retry <= 0) Future.failed(RedisClusterException(s"cluster_state not ok, aborting cache update after $maxRetries attempts"))
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
    val delayedF = Promise[A]()
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
   * @param remainingTimeout how much longer to retry an action, rather than number of retries
   * @tparam A response type
   * @return
   */
  private def send[A](request: Request[A], server: Server,
                      triedServers: Set[Server] = Set.empty,
                      retry: Int = maxRetries, remainingTimeout: Duration = connectTimeout,
                      error: Option[RedisException] = None): Future[A] = {
    if (retry <= 0) Future.failed(RedisIOException(s"Gave up on request after $maxRetries retries: $request", error.orNull))
    else {
      val (connection,_) = connections.getOrElse(server, {
        val con = makeConnection(server)
        connections = this.synchronized {
          connections.updated(server, (con,0))
        }
        (con,0)
      })

      // handle cases that should be retried (MOVE, ASK, TRYAGAIN)
      connection.send(request).recoverWith {

        case err @ RedisClusterErrorResponseException(Moved(slot, host, port), _) =>
          request.reset()
          // this will usually happen when cache is missed.
          val movedServer: Server = Server(host, port)
          updateHashMisses(slot, movedServer)

          send(request, movedServer, triedServers+server, retry - 1, remainingTimeout, Option(err))

        case err @ RedisClusterErrorResponseException(Ask(hashSlot, host, port), _) =>
          request.reset()
          val askServer = Server(host,port)
          send(request, askServer, triedServers+server, retry - 1, remainingTimeout, Option(err))

        case err @ RedisClusterErrorResponseException(TryAgain, _) =>
          request.reset()
          // TODO what is actually the intended semantics of TryAgain?
          delayed(tryAgainWait) { send(request, server, triedServers+server, retry - 1, remainingTimeout - tryAgainWait, Option(err)) }

        case err @ RedisClusterErrorResponseException(ClusterDown, _) =>
          request.reset()
          logger.debug(s"Received CLUSTERDOWN error from request $request. Retrying ...")
          val nextTimeout = remainingTimeout - clusterDownWait
          if (nextTimeout <= 0.millis) Future.failed(RedisIOException(s"Aborted request $request after trying for ${connectTimeout}", err))
          else delayed(clusterDownWait) { send(request, server, triedServers, retry, nextTimeout, Option(err)) }
          // wait a bit because the cluster may not be fully initialized or fixing itself

        case err @ RedisIOException(message, cause) =>
          request.reset()
          updateServerErrors(server)
          val nextTriedServers = triedServers + server
          // try any server that isn't one we tried already
          connections.keys.find { s => !nextTriedServers.contains(s)  } match {
            case Some(nextServer) => send(request, nextServer, nextTriedServers, retry - 1, remainingTimeout, Option(err))
            case None => Future.failed(RedisIOException("No valid connection available.", err))
          }
      }
    }
  }

  private def updateHashMisses(slot: Int, newServer: Server) = this.synchronized {
    // I believe it is safe to synchronize only updates to hashSlots.
    // If another concurrent read is outdated it will be redirected anyway and potentially repeat the update.
    hashMisses += 1

    // do this update no matter what, updating cache may take a bit
    hashSlots = hashSlots.updated(slot, Option(newServer))

    if (hashMisses > maxHashMisses) {
      hashMisses = 0
      updateCache(maxRetries)
    }
  }

  private def updateServerErrors(server: Server): Unit = this.synchronized {
    for {
      (con, errors) <- connections.get(server)
    } {
      val nextConnections =
        if (errors >= maxConnectionMisses) connections - server
        else connections.updated(server, (con,errors+1))

      this.connections = nextConnections
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
          case Some(server) => send(req, server)
        }

      case keyReq: Request[A] with Key =>
        hashSlots(ClusterCRC16.getSlot(keyReq.key)) match {
          case None =>
            if (connections.isEmpty) Future.failed(RedisIOException("No cluster node connection available"))
            else send(keyReq, connections.head._1) // when we can't get a cached server, just get the first connection
          case Some(server) =>
            send(keyReq, server)
        }

      case clusterReq: Request[A] with Cluster =>
        // requests that are valid with any cluster node
        val server = connections.head._1
        send(clusterReq, server)
      case _ =>
        // TODO what to do about non-key requests? they would be valid for any individual cluster node,
        // but arbitrary choice is probably not what's intended..
        Future.failed(RedisInvalidArgumentException("This command is not supported for clusters"))
    }

  // TODO at init: fetch all hash slot-node associations: CLUSTER SLOTS
}
