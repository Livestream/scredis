package scredis.io

import scredis.protocol._
import scredis.{Subscription, Transaction}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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