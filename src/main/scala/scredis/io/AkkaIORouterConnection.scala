package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._
import akka.routing._

import scredis.Transaction
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.ServerRequests.Shutdown

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

/**
 * This trait represents a connection to a `Redis` server.
 */
abstract class AkkaIORouterConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  decodersCount: Int = 3,
  receiveTimeout: FiniteDuration = 5 seconds
) extends TransactionEnabledConnection with LazyLogging {
  
  @volatile private var isClosed = false
  
  protected implicit val listeners = system.actorOf(
    RoundRobinPool(2).props(
      Props(
        classOf[ListenerActor],
        host,
        port,
        passwordOpt,
        database,
        decodersCount,
        receiveTimeout
      ).withDispatcher(
        "scredis.akka.listener-dispatcher"
      )
    ),
    Connection.getUniqueName(s"listener-actor-$host-$port")
  )
  
  override protected implicit val ec = system.dispatcher
  
  override protected def send[A](request: Request[A]): Future[A] = {
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
  ): Future[Vector[Try[Any]]] = {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      logger.debug(s"Sending transaction: $transaction")
      Protocol.send(transaction)
    }
  }
  
}