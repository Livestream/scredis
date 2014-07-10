package scredis

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.io._
import scredis.protocol._

import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

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
    AbstractClient.getUniqueName(s"io-actor-$host-$port")
  )
  private val partitionerActor = system.actorOf(
    Props(classOf[PartitionerActor], ioActor).withDispatcher("scredis.partitioner-dispatcher"),
    AbstractClient.getUniqueName(s"partitioner-actor-$host-$port")
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

object AbstractClient {
  private val ids = MMap[String, AtomicInteger]()
  
  private def getUniqueName(name: String): String = {
    val counter = ids.synchronized {
      ids.getOrElseUpdate(name, new AtomicInteger(0))
    }
    s"$name-${counter.incrementAndGet()}"
  }
  
}