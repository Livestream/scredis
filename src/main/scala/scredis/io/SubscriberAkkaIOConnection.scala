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
  database: Int,
  decodersCount: Int = 3,
  receiveTimeout: FiniteDuration = 5 seconds
) extends SubscriberConnection with LazyLogging {
  
  import system.dispatcher
  
  @volatile private var isClosed = false
  
  protected val listenerActor = system.actorOf(
    Props(
      classOf[SubscriberListenerActor],
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
  
  override protected def sendAsSubscriber(request: Request[_]): Future[Int] = {
    if (isClosed) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      listenerActor ! request
      request.future.asInstanceOf[Future[Int]]
    }
  }
  
  override protected def updateSubscription(subscription: Subscription): Unit = {
    listenerActor ! SubscriberListenerActor.Subscribe(subscription)
  }
  
  protected def close(): Future[Unit] = {
    isClosed = true
    val unsubscribe = Unsubscribe()
    val pUnsubscribe = PUnsubscribe()
    val quit = Quit()
    listenerActor ! unsubscribe
    listenerActor ! pUnsubscribe
    
    unsubscribe.future.andThen {
      case _ => pUnsubscribe.future.andThen {
        case _ => {
          listenerActor ! SubscriberListenerActor.Shutdown(quit)
        }
      }
    }
    
    quit.future
  }
  
}