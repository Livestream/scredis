package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.{ Transaction, Subscription, PubSubMessage }
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.ServerRequests.Shutdown

import scala.util.Try
import scala.collection.mutable.{ Map => MMap }
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

trait Connection {
  protected implicit val ec: ExecutionContext
}

trait NonBlockingConnection {
  protected def send[A](request: Request[A]): Future[A]
}

trait TransactionEnabledConnection {
  protected def send(transaction: Transaction): Future[Vector[Try[Any]]]
}

trait BlockingConnection {
  protected def sendBlocking[A](request: Request[A])(implicit timeout: Duration): A
}

trait SubscriberConnection {
  protected def sendAsSubscriber(request: Request[_]): Future[Int]
  protected def updateSubscription(subscription: Subscription): Unit
}

object Connection {
  private val ids = MMap[String, AtomicInteger]()
  
  private[io] def getUniqueName(name: String): String = {
    val counter = ids.synchronized {
      ids.getOrElseUpdate(name, new AtomicInteger(0))
    }
    s"$name-${counter.incrementAndGet()}"
  }
  
}