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

/**
 * This trait represents a connection to a `Redis` server.
 */
abstract class AkkaIOConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int
) extends TransactionEnabledConnection with LazyLogging {
  
  @volatile protected var isClosed = false
  
  protected val ioActor = system.actorOf(
    Props(
      classOf[IOActor], new InetSocketAddress(host, port), passwordOpt, database
    ).withDispatcher(
      "scredis.io-dispatcher"
    ),
    Connection.getUniqueName(s"io-actor-$host-$port")
  )
  protected val partitionerActor = system.actorOf(
    Props(classOf[PartitionerActor], ioActor).withDispatcher("scredis.partitioner-dispatcher"),
    Connection.getUniqueName(s"partitioner-actor-$host-$port")
  )
  
  override protected implicit val ec = system.dispatcher
  
  protected def send[A](request: Request[A]): Future[A] = {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      logger.debug(s"Sending request: $request")
      request match {
        case _: Quit      => isClosed = true
        case _: Shutdown  => isClosed = true
        case _            =>
      }
      partitionerActor ! request
      request.future
    }
  }
  
  protected def sendTransaction(transaction: Transaction): Future[IndexedSeq[Try[Any]]] = {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      logger.debug(s"Sending transaction: $transaction")
      partitionerActor ! transaction
      transaction.future
    }
  }
  
  protected def sendBlocking[A](request: Request[A]): A = {
    logger.debug(s"Sending blocking request: $request")
    // TODO: send
    Await.result(request.future, Duration.Inf)
  }
  
  ioActor ! partitionerActor
  
}