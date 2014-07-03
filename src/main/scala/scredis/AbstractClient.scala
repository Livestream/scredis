package scredis

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.io._
import scredis.protocol._

import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress

/**
 * This trait represents an instance of a client connected to a `Redis` server.
 */
abstract class AbstractClient(
  system: ActorSystem,
  host: String,
  port: Int
) extends LazyLogging {
  
  private val ioActor = system.actorOf(
    Props(classOf[IOActor], new InetSocketAddress(host, port)).withDispatcher(
      "scredis.io-dispatcher"
    ),
    "IOActor"
  )
  private val partitionerActor = system.actorOf(
    Props(classOf[PartitionerActor], ioActor).withDispatcher("scredis.partitioner-dispatcher"),
    "PartitionerActor"
  )
  
  implicit val dispatcher: ExecutionContext = system.dispatcher
  
  protected def send[A](request: Request[A]): Future[A] = {
    logger.debug(s"Sending request: $request")
    partitionerActor ! request
    request.future
  }
  
  protected def sendBlocking[A](request: Request[A]): A = {
    logger.debug(s"Sending blocking request: $request")
    // TODO: send
    Await.result(request.future, Duration.Inf)
  }
  
  ioActor ! partitionerActor
  
}