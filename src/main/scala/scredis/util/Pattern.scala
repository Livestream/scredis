package scredis.util

import scredis.exceptions._

import scala.concurrent.duration.FiniteDuration

/**
 * Provides a retry pattern.
 */
private[scredis] object Pattern {

  private[scredis] def retry[A](tries: Int, sleep: Option[FiniteDuration])(op: Int => A)(
    implicit logger: Logger
  ): A = {
    var count = 0
    var result: Option[A] = None
    while (result.isEmpty) {
      try {
        count += 1
        result = Some(op(count))
      } catch {
        case e: RedisCommandException => throw e
        case e: RedisParsingException => throw e
        case e: Throwable => if (count >= tries) {
          throw e
        } else {
          logger.error("A retriable error occurred while executing command", e)
          sleep.map(d => Thread.sleep(d.toMillis))
        }
      }
    }
    result.get
  }

}