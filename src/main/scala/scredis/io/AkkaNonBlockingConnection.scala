package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.Transaction
import scredis.exceptions._
import scredis.protocol._

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * This trait represents a connection to a `Redis` server.
 */
abstract class AkkaNonBlockingConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  decodersCount: Int = 3,
  receiveTimeoutOpt: Option[FiniteDuration] = Some(5 seconds)
) extends AbstractAkkaConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = database,
  decodersCount = decodersCount,
  receiveTimeoutOpt = receiveTimeoutOpt
) with NonBlockingConnection with TransactionEnabledConnection {
  
  private val lock = new ReentrantReadWriteLock()
  
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
  
  override protected def send[A](request: Request[A]): Future[A] = {
    if (isShuttingDown) {
      Future.failed(RedisIOException("Connection has been shutdown"))
    } else {
      logger.debug(s"Sending request: $request")
      updateState(request)
      Protocol.send(request)
    }
  }
  
  override protected def send(transaction: Transaction): Future[Vector[Try[Any]]] = {
    if (isShuttingDown) {
      Future.failed(RedisIOException("Connection has been shutdown"))
    } else {
      logger.debug(s"Sending transaction: $transaction")
      transaction.requests.foreach(updateState)
      Protocol.send(transaction)
    }
  }
  
}