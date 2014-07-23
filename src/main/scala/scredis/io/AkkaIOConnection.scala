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
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * This trait represents a connection to a `Redis` server.
 */
abstract class AkkaIOConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  decodersCount: Int = 3,
  receiveTimeout: FiniteDuration = 5 seconds
) extends TransactionEnabledConnection with BlockingConnection with LazyLogging {
  
  @volatile private var isClosed = false
  
  private val lock = new ReentrantReadWriteLock()
  
  protected implicit val listenerActor = system.actorOf(
    Props(
      classOf[ListenerActor],
      host,
      port,
      passwordOpt,
      database,
      decodersCount,
      receiveTimeout
    ).withDispatcher(
      "scredis.listener-dispatcher"
    ),
    Connection.getUniqueName(s"listener-actor-$host-$port")
  )
  
  override protected implicit val ec = system.dispatcher
  
  private def withReadLock[A](f: => Future[A]): Future[A] = {
    if (lock.readLock.tryLock) {
      try {
        f
      } finally {
        lock.readLock.unlock()
      }
    } else {
      Future.failed(RedisIOException("Trying to send request on a blocked connection"))
    }
  }
  
  private def withWriteLock[A](f: => A): A = {
    lock.writeLock.lock()
    try {
      f
    } finally {
      lock.writeLock.unlock()
    }
  }
  
  override protected def send[A](request: Request[A]): Future[A] = withReadLock {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      logger.debug(s"Sending request: $request")
      request match {
        case _: Quit      => isClosed = true
        case _: Shutdown  => isClosed = true
        case _            =>
      }
      Protocol.send(request)
    }
  }
  
  override protected def sendTransaction(
    transaction: Transaction
  ): Future[Vector[Try[Any]]] = withReadLock {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      logger.debug(s"Sending transaction: $transaction")
      Protocol.send(transaction)
    }
  }
  
  override protected def sendBlocking[A](request: Request[A])(
    implicit timeout: Duration
  ): A = withWriteLock {
    logger.debug(s"Sending blocking request: $request")
    listenerActor ! request
    Await.result(request.future, timeout)
  }
  
}