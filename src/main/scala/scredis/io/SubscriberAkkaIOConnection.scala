package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.{ Subscription, PubSubMessage }
import scredis.protocol._
import scredis.protocol.requests.PubSubRequests.{ Unsubscribe, PUnsubscribe }

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress

/**
 * This trait represents a subscriber connection to a `Redis` server.
 */
abstract class SubscriberAkkaIOConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int
) extends SubscriberConnection with LazyLogging {
  
  @volatile private var isClosed = false
  
  protected val ioActor = system.actorOf(
    Props(
      classOf[SubscriberIOActor], new InetSocketAddress(host, port), passwordOpt, database
    ).withDispatcher(
      "scredis.io-dispatcher"
    ),
    Connection.getUniqueName(s"io-subscriber-actor-$host-$port")
  )
  protected val partitionerActor = system.actorOf(
    Props(classOf[PartitionerActor], ioActor).withDispatcher("scredis.partitioner-dispatcher"),
    Connection.getUniqueName(s"partitioner-actor-$host-$port")
  )
  
  override protected def sendAsSubscriber(request: Request[_]): Future[Int] = {
    partitionerActor ! request
    request.future.asInstanceOf[Future[Int]]
  }
  
  override protected def updateSubscription(subscription: Subscription): Unit = {
    partitionerActor ! PartitionerActor.Subscribe(subscription)
  }
  
  protected def close(): Unit = {
    isClosed = true
    sendAsSubscriber(Unsubscribe())
    sendAsSubscriber(PUnsubscribe())
    partitionerActor ! PartitionerActor.CloseIOActor
  }
  
  ioActor ! partitionerActor
  
}