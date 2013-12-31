/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package scredis.util

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration

import org.scalatest.matchers.MustMatchers._

object TestUtils {
  
  final class RichFuture[A](future: Future[A]) {
    def ! = Await.result(future, Duration.Inf)
  }
  
  implicit def futureToRichFuture[A](f: Future[A]): RichFuture[A] = new RichFuture(f)
  
  implicit def stringFutureToMatcher(f: Future[String]): StringMustWrapper = {
    convertToStringMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def intFutureToMatcher(f: Future[Int]): IntMustWrapper = {
    convertToIntMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def longFutureToMatcher(f: Future[Long]): LongMustWrapper = {
    convertToLongMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def floatFutureToMatcher(f: Future[Float]): FloatMustWrapper = {
    convertToFloatMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def doubleFutureToMatcher(f: Future[Double]): DoubleMustWrapper = {
    convertToDoubleMustWrapper(Await.result(f, Duration.Inf))
  }
  
  implicit def traversableFutureToMatcher[A](
    f: Future[_ <: Traversable[A]]
  ): TraversableMustWrapper[A] = {
    convertToTraversableMustWrapper(Await.result(f, Duration.Inf))
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
  
  def evaluating[A](f: => Future[A]): ResultOfEvaluatingApplication =
    org.scalatest.matchers.MustMatchers.evaluating(Await.result(f, Duration.Inf))
    
  def evaluatingSync[A](f: => A): ResultOfEvaluatingApplication =
    org.scalatest.matchers.MustMatchers.evaluating(f)
  
}