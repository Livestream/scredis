package scredis.io

import com.typesafe.scalalogging.LazyLogging

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
  implicit val dispatcher: ExecutionContext
}

trait NonBlockingConnection {
  protected[scredis] def send[A](request: Request[A]): Future[A]
}

trait TransactionEnabledConnection {
  protected[scredis] def send(transaction: Transaction): Future[Vector[Try[Any]]]
}

trait BlockingConnection {
  protected[scredis] def sendBlocking[A](request: Request[A])(implicit timeout: Duration): Try[A]
}

trait SubscriberConnection {
  protected[scredis] def sendAsSubscriber(request: Request[_]): Future[Int]
  protected[scredis] def setSubscription(subscription: Subscription): Unit
}