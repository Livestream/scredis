package scredis.util

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration

import org.scalatest.MustMatchers._

object TestUtils {
  
  final class RichFuture[A](future: Future[A]) {
    def ! = Await.result(future, Duration.Inf)
  }
  
  implicit def futureToRichFuture[A](f: Future[A]): RichFuture[A] = new RichFuture(f)
  
  implicit def stringFutureToMatcher(f: Future[String]): StringMustWrapper = {
    convertToStringMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def intFutureToMatcher(f: Future[Int]): AnyMustWrapper[Int] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def longFutureToMatcher(f: Future[Long]): AnyMustWrapper[Long] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def floatFutureToMatcher(f: Future[Float]): AnyMustWrapper[Float] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def doubleFutureToMatcher(f: Future[Double]): AnyMustWrapper[Double] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def traversableFutureToMatcher[A](
    f: Future[_ <: Traversable[A]]
  ): AnyMustWrapper[_ <: Traversable[A]] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }

  implicit def booleanFutureToMatcher(f: Future[Boolean]): AnyMustWrapper[Boolean] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def optionFutureToMatcher[A](f: Future[Option[A]]): AnyMustWrapper[Option[A]] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def eitherFutureToMatcher[A, B](
    f: Future[Either[A, B]]
  ): AnyMustWrapper[Either[A, B]] = {
    convertToAnyMustWrapper(Await.result(f, Duration.Inf))
  }
}
