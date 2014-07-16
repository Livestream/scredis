package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.{ Subscription, PubSubMessage }
import scredis.protocol._
import scredis.protocol.requests.PubSubRequests.{ Unsubscribe, PUnsubscribe }
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.exceptions.RedisIOException

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
  
  import system.dispatcher
  
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
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      partitionerActor ! request
      request.future.asInstanceOf[Future[Int]]
    }
  }
  
  override protected def updateSubscription(subscription: Subscription): Unit = {
    partitionerActor ! PartitionerActor.Subscribe(subscription)
  }
  
  protected def close(): Future[Unit] = {
    isClosed = true
    val unsubscribe = Unsubscribe()
    val pUnsubscribe = PUnsubscribe()
    val quit = Quit()
    partitionerActor ! unsubscribe
    partitionerActor ! pUnsubscribe
    
    unsubscribe.future.andThen {
      case _ => pUnsubscribe.future.andThen {
        case _ => {
          partitionerActor ! PartitionerActor.ResetPubSub
          partitionerActor ! quit
        }
      }
    }
    
    quit.future
  }
  
  ioActor ! partitionerActor
  
}