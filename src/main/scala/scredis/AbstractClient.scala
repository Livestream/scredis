package scredis

import com.typesafe.scalalogging.Logging

import scredis.protocol._

import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

/**
 * This trait represents an instance of a client connected to a `Redis` server.
 */
abstract class AbstractClient extends Logging {
  
  protected implicit val ec = ExecutionContext.global
  
  protected def send[A](request: Request[A]): Future[A] = {
    logger.debug(s"Sending request: $request")
    // TODO: send
    request.future
  }
  
  protected def sendBlocking[A](request: Request[A]): A = {
    logger.debug(s"Sending blocking request: $request")
    // TODO: send
    Await.result(request.future, Duration.Inf)
  }
  
}