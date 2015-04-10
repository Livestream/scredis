package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor._

import scredis.{ Subscription, PubSubMessage }
import scredis.protocol._
import scredis.protocol.requests.PubSubRequests._
import scredis.protocol.requests.ConnectionRequests.{ Auth, Quit }
import scredis.protocol.requests.ServerRequests.ClientSetName
import scredis.exceptions.RedisIOException
import scredis.util.UniqueNameGenerator

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
  nameOpt: Option[String],
  decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaListenerDispatcherPath: String,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String
) extends AbstractAkkaConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = 0,
  nameOpt = nameOpt,
  decodersCount = decodersCount,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
) with SubscriberConnection {
  
  private val lock = new Semaphore(1)
  
  protected val listenerActor = system.actorOf(
    Props(
      classOf[SubscriberListenerActor],
      host,
      port,
      passwordOpt,
      nameOpt,
      decodersCount,
      receiveTimeoutOpt,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint,
      akkaIODispatcherPath,
      akkaDecoderDispatcherPath
    ).withDispatcher(akkaListenerDispatcherPath),
    UniqueNameGenerator.getUniqueName(s"${nameOpt.getOrElse(s"$host-$port")}-listener-actor")
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
  
  override protected[scredis] def sendAsSubscriber(request: Request[_]): Future[Int] = {
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
  
  override protected[scredis] def setSubscription(subscription: Subscription): Unit = {
    listenerActor ! SubscriberListenerActor.Subscribe(subscription)
  }
  
  protected def authenticate(password: String): Future[Unit] = {
    lock.acquire()
    val auth = Auth(password)
    listenerActor ! SubscriberListenerActor.SaveSubscriptions
    unsubscribeAndThen {
      listenerActor ! SubscriberListenerActor.SendAsRegularClient(auth)
    }
    auth.future.onComplete {
      case _ => {
        listenerActor ! SubscriberListenerActor.RecoverPreviousSubscriberState
        lock.release()
      }
    }
    auth.future
  }
  
  protected def setName(name: String): Future[Unit] = {
    lock.acquire()
    val setName = ClientSetName(name)
    listenerActor ! SubscriberListenerActor.SaveSubscriptions
    unsubscribeAndThen {
      listenerActor ! SubscriberListenerActor.SendAsRegularClient(setName)
    }
    setName.future.onComplete {
      case _ => {
        listenerActor ! SubscriberListenerActor.RecoverPreviousSubscriberState
        lock.release()
      }
    }
    setName.future
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