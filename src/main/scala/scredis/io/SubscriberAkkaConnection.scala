package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.{ Subscription, PubSubMessage }
import scredis.protocol._
import scredis.protocol.requests.PubSubRequests._
import scredis.protocol.requests.ConnectionRequests.{ Auth, Quit }
import scredis.exceptions.RedisIOException

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.util.concurrent.Semaphore
import java.net.InetSocketAddress

/**
 * This trait represents a subscriber connection to a `Redis` server.
 */
abstract class SubscriberAkkaConnection(
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
) with SubscriberConnection {
  
  private val lock = new Semaphore(1)
  
  protected val listenerActor = system.actorOf(
    Props(
      classOf[SubscriberListenerActor],
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
  
  private def unsubscribeAndThen(f: => Any): Unit = {
    val unsubscribe = Unsubscribe()
    val pUnsubscribe = PUnsubscribe()
    listenerActor ! unsubscribe
    unsubscribe.future.recover {
      case e: Throwable => -1
    }.flatMap { _ =>
      listenerActor ! pUnsubscribe
      pUnsubscribe.future.recover {
        case e: Throwable => -1
      }.map { _ =>
        f
      }
    }
  }
  
  override protected def sendAsSubscriber(request: Request[_]): Future[Int] = {
    lock.acquire()
    if (isShuttingDown) {
      lock.release()
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      listenerActor ! request
      request.future.onComplete {
        case _ => lock.release()
      }
      request.future.asInstanceOf[Future[Int]]
    }
  }
  
  override protected def setSubscription(subscription: Subscription): Unit = {
    listenerActor ! SubscriberListenerActor.Subscribe(subscription)
  }
  
  protected def authenticate(password: String): Future[Unit] = {
    lock.acquire()
    val auth = Auth(password)
    listenerActor ! SubscriberListenerActor.SaveSubscriptions
    unsubscribeAndThen {
      listenerActor ! SubscriberListenerActor.Authenticate(auth)
    }
    auth.future.onComplete {
      case _ => {
        listenerActor ! SubscriberListenerActor.Authenticated
        lock.release()
      }
    }
    auth.future
  }
  
  protected def shutdown(): Future[Unit] = {
    lock.acquire()
    isShuttingDown = true
    val quit = Quit()
    unsubscribeAndThen {
      listenerActor ! SubscriberListenerActor.Shutdown(quit)
    }
    quit.future.onComplete {
      case _ => lock.release()
    }
    quit.future
  }
  
}