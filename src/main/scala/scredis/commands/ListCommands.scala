package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.ListRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements list commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ListCommands { self: NonBlockingConnection =>
  
  /**
   * Returns an element from a list by its index.
   *
   * @note The index is zero-based, so 0 means the first element, 1 the second element and so on.
   * Negative indices can be used to designate elements starting at the tail of the list.
   * Here, -1 means the last element, -2 means the penultimate and so forth.
   *
   * @param key list key
   * @param index zero-based position in the list
   * @return the requested element, or $none when index is out of range
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lIndex[K: Writer, R: Reader](key: K, index: Long): Future[Option[R]] = send(LIndex(key, index))
  
  /**
   * Inserts an element before or after another element in a list.
   *
   * @param key list key
   * @param pivot value after/before which the element should be inserted
   * @param value element to be inserted
   * @param after when true, inserts the element after the pivot, when false the element is inserted
   * before the pivot (default is true, i.e. after)
   * @return the length of the list after the insert operation, or None if the index is out of range
   * @throws $e if key contains a non-list value
   *
   * @since 2.2.0
   */
  def lInsert[K: Writer, W1: Writer, W2: Writer](
    key: K,
    position: scredis.Position,
    pivot: W1,
    value: W2
  ): Future[Option[Long]] = send(LInsert(key, position, pivot, value))
  
  /**
   * Returns the length of a list.
   *
   * @param key list key
   * @return the length of the list at key, or 0 if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lLen[K: Writer](key: K): Future[Long] = send(LLen(key))
  
  /**
   * Removes and returns the first element of a list.
   *
   * @param key list key
   * @return the popped element, or $none if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lPop[K: Writer, R: Reader](key: K): Future[Option[R]] = send(LPop(key))
  
  /**
   * Prepends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param key list key
   * @param values value(s) to prepend
   * @return the length of the list after the push operations
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lPush[K: Writer, W: Writer](key: K, values: W*): Future[Long] = send(LPush(key, values: _*))
  
  /**
   * Prepends a value to a list, only if the list exists.
   *
   * @param key list key
   * @param value value to prepend
   * @return the length of the list after the push operation
   * @throws $e if key contains a non-list value
   *
   * @since 2.2.0
   */
  def lPushX[K: Writer, W: Writer](key: K, value: W): Future[Long] = send(LPushX(key, value))
  
  /**
   * Returns a range of elements from a list.
   *
   * @note The offsets start and end are zero-based indexes, with 0 being the first element of the
   * list (the head of the list), 1 being the next element and so on. These offsets can also be
   * negative numbers indicating offsets starting at the end of the list. For example, -1 is the
   * last element of the list, -2 the penultimate, and so on. Both offsets are inclusive, i.e.
   * LRANGE key 0 10 will return 11 elements (if they exist).
   *
   * @param key list key
   * @param start start offset (inclusive)
   * @param stop stop offset (inclusive)
   * @return list of elements in the specified range, or the empty list if there are no such
   * elements or the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lRange[K: Writer, R: Reader](key: K, start: Long = 0, stop: Long = -1): Future[List[R]] = send(
    LRange[K, R, List](key, start, stop)
  )
  
  /**
   * Removes the first count occurrences of elements equal to value from the list stored at key.
   *
   * @note The count argument influences the operation in the following ways:
   * {{{
   * count > 0: Remove elements equal to value moving from head to tail.
   * count < 0: Remove elements equal to value moving from tail to head.
   * count = 0: Remove all elements equal to value.
   * }}}
   *
   * @param key list key
   * @param value value to be removed from the list
   * @param count indicates the number of found values that should be removed, see above note
   * @return the number of removed elements
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lRem[K: Writer, W: Writer](key: K, value: W, count: Int = 0): Future[Long] = send(
    LRem(key, count, value)
  )
  
  /**
   * Sets the value of an element in a list by its index.
   *
   * @param key list key
   * @param index position of the element to set
   * @param value value to be set at index
   * @throws $e if index is out of range or if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lSet[K: Writer, W: Writer](key: K, index: Long, value: W): Future[Unit] = send(
    LSet(key, index, value)
  )
  
  /**
   * Trims a list to the specified range.
   *
   * @note Out of range indexes will not produce an error: if start is larger than the end of the
   * list, or start > end, the result will be an empty list (which causes key to be removed). If
   * end is larger than the end of the list, Redis will treat it like the last element of the list.
   *
   * @param key list key
   * @param start start offset (inclusive)
   * @param stop stop offset (inclusive)
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lTrim[K: Writer](key: K, start: Long, stop: Long): Future[Unit] = send(
    LTrim(key, start, stop)
  )
  
  /**
   * Removes and returns the last element of a list.
   *
   * @param key list key
   * @return the popped element, or $none if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def rPop[K: Writer, R: Reader](key: K): Future[Option[R]] = send(RPop(key))
  
  /**
   * Removes the last element in a list, appends it to another list and returns it.
   *
   * @param sourceKey key of list to be pop from
   * @param destKey key of list to be push to
   * @return the popped element, or $none if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.2.0
   */
  def rPopLPush[KS: Writer, KD: Writer, R: Reader](sourceKey: KS, destKey: KD): Future[Option[R]] = send(
    RPopLPush(sourceKey, destKey)
  )
  
  /**
   * Appends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param key list key
   * @param values value(s) to prepend
   * @return the length of the list after the push operations
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def rPush[K: Writer, W: Writer](key: K, values: W*): Future[Long] = send(
    RPush(key, values: _*)
  )
  
  /**
   * Appends a value to a list, only if the list exists.
   *
   * @param key list key
   * @param value value to prepend
   * @return the length of the list after the push operation
   * @throws $e if key contains a non-list value
   *
   * @since 2.2.0
   */
  def rPushX[K: Writer, W: Writer](key: K, value: W): Future[Long] = send(RPushX(key, value))
  
}