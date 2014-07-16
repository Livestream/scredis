package scredis.commands

import scredis.io.BlockingConnection
import scredis.protocol.requests.ListRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements blocking list commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait BlockingListCommands { self: BlockingConnection =>
  
  private implicit def secondsToDuration(seconds: Int): Duration = if (seconds <= 0) {
    Duration.Inf
  } else {
    (seconds + 2).seconds
  }
  
  /**
   * Removes and returns the first element in a list, or block until one is available.
   *
   * @param timeoutSeconds timeout in seconds, if zero, the command blocks indefinitely until
   * an element is available
   * @param keys list key(s)
   * @return list of key to popped element pair, or $none if timeout occurs
   * @throws $e if a key contains a non-list value
   *
   * @since 2.0.0
   */
  def blPop[R: Reader](timeoutSeconds: Int, keys: String*): Option[(String, R)] = sendBlocking(
    BLPop(timeoutSeconds, keys: _*)
  )(timeoutSeconds)
  
  /**
   * Removes and returns the last element in a list, or block until one is available.
   *
   * @param timeoutSeconds timeout in seconds, if zero, the command blocks indefinitely until
   * an element is available in at least one of the provided lists
   * @param keys list key(s)
   * @return list of key to popped element pair, or $none if timeout occurs
   * @throws $e if a key contains a non-list value
   *
   * @since 2.0.0
   */
  def brPop[R: Reader](timeoutSeconds: Int, keys: String*): Option[(String, R)] = sendBlocking(
    BRPop(timeoutSeconds, keys: _*)
  )(timeoutSeconds)
  
  /**
   * Pops a value from a list, pushes it to another list and returns it, or block until one is
   * available.
   *
   * @param sourceKey key of list to pop from
   * @param destKey key of list to push to
   * @param timeoutSeconds timeout in seconds, if zero, the command blocks indefinitely until
   * an element is available in the list at sourceKey
   * @return the element being popped from source and pushed to destination, or $none if timeout
   * occurs
   * @throws $e if sourceKey or destKey contain non-list values
   *
   * @since 2.2.0
   */
  def brPopLPush[R: Reader](
    sourceKey: String, destKey: String, timeoutSeconds: Int
  ): Option[R] = sendBlocking(BRPopLPush(sourceKey, destKey, timeoutSeconds))(timeoutSeconds)
  
}