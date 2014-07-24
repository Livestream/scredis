package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.Transaction
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.ServerRequests.Shutdown

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

/**
 * This trait represents a blocking connection to a `Redis` server.
 */
abstract class AkkaBlockingConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  decodersCount: Int = 3
) extends AbstractAkkaConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = database,
  decodersCount = decodersCount,
  receiveTimeoutOpt = None
) with BlockingConnection {
  
  private val lock = new ReentrantLock()
  
  protected implicit val listenerActor = system.actorOf(
    Props(
      classOf[ListenerActor],
      host,
      port,
      passwordOpt,
      database,
      decodersCount,
      receiveTimeoutOpt
    ).withDispatcher(
      "scredis.akka.listener-dispatcher"
    ),
    Connection.getUniqueName(s"listener-actor-$host-$port")
  )
  
  private def withLock[A](f: => A): A = {
    if (lock.tryLock) {
      try {
        f
      } finally {
        lock.unlock()
      }
    } else {
      throw RedisIOException("Trying to send request on a blocked connection")
    }
  }
  
  override protected def sendBlocking[A](request: Request[A])(
    implicit timeout: Duration
  ): A = withLock {
    logger.debug(s"Sending blocking request: $request")
    updateState(request)
    val future = Protocol.send(request)
    Await.result(future, timeout)
  }
  
}