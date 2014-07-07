package scredis.util

import akka.actor.ActorSystem

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration

object TestUtils {
  implicit val system = ActorSystem("test")
  
  final implicit class RichFuture[A](future: Future[A]) {
    def ! = Await.result(future, Duration.Inf)
  }
  
}
