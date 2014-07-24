package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.{ Subscription, PubSubMessage }
import scredis.protocol._
import scredis.protocol.requests.PubSubRequests.{ Unsubscribe, PUnsubscribe }
import scredis.protocol.requests.ConnectionRequests.{ Auth, Quit }
import scredis.exceptions.RedisIOException

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.util.concurrent.locks.ReentrantReadWriteLock
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
  
  private val lock = new ReentrantReadWriteLock()
  
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
  
  private def withReadLock[A](f: => A): A = {
    lock.readLock.lock()
    try {
      f
    } finally {
      lock.readLock.unlock()
    }
  }
  
  private def unsubscribeAndThen(f: => Any): Unit = {
    val unsubscribe = Unsubscribe()
    val pUnsubscribe = PUnsubscribe()
    listenerActor ! unsubscribe
    val future = unsubscribe.future.recover {
      case e: Throwable => -1
    }.flatMap { _ =>
      listenerActor ! pUnsubscribe
      pUnsubscribe.future.recover {
        case e: Throwable => -1
      }.map { _ =>
        f
      }
    }
    Await.result(future, 5 seconds)
  }
  
  override protected def sendAsSubscriber(request: Request[_]): Future[Int] = withReadLock {
    if (isShuttingDown) {
      Future.failed(RedisIOException("Connection has been closed"))
    } else {
      listenerActor ! request
      request.future.asInstanceOf[Future[Int]]
    }
  }
  
  override protected def updateSubscription(subscription: Subscription): Unit = {
    listenerActor ! SubscriberListenerActor.Subscribe(subscription)
  }
  
  protected def authenticate(password: String)(implicit timeout: Duration): Unit = {
    lock.writeLock.lock()
    val auth = Auth(password)
    listenerActor ! SubscriberListenerActor.SaveSubscriptions
    try {
      unsubscribeAndThen {
        listenerActor ! SubscriberListenerActor.Authenticate(auth)
      }
      Await.result(auth.future, timeout)
      listenerActor ! SubscriberListenerActor.Authenticated
    } finally {
      lock.writeLock.unlock()
    }
  }
  
  protected def shutdown()(implicit timeout: Duration): Unit = {
    lock.writeLock.lock()
    isShuttingDown = true
    val quit = Quit()
    try {
      unsubscribeAndThen {
        listenerActor ! SubscriberListenerActor.Shutdown(quit)
      }
      Await.result(quit.future, timeout)
    } finally {
      lock.writeLock.unlock()
    }
  }
  
}