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
package scredis.commands.async

import scredis.CommandOptions
import scredis.parsing._

import scala.concurrent.Future

/**
 * This trait implements asynchronous lists commands.
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait ListsCommands extends Async {

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
  def lIndex[A](key: String, index: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.lIndex(key, index))

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
  def lInsert(key: String, pivot: String, value: Any, after: Boolean = true)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Option[Long]] = async(_.lInsert(key, pivot, value, after))

  /**
   * Inserts an element before another element in a list.
   *
   * @param key list key
   * @param pivot value after/before which the element should be inserted
   * @param value element to be inserted
   * @return the length of the list after the insert operation, or None if the index is out of range
   * @throws $e if key contains a non-list value
   *
   * @since 2.2.0
   */
  def lInsertBefore(key: String, pivot: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Option[Long]] = async(_.lInsertBefore(key, pivot, value))

  /**
   * Inserts an element after another element in a list.
   *
   * @param key list key
   * @param pivot value after/before which the element should be inserted
   * @param value element to be inserted
   * @return the length of the list after the insert operation, or None if the index is out of range
   * @throws $e if key contains a non-list value
   *
   * @since 2.2.0
   */
  def lInsertAfter(key: String, pivot: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Option[Long]] = async(_.lInsertAfter(key, pivot, value))

  /**
   * Returns the length of a list.
   *
   * @param key list key
   * @return the length of the list at key, or 0 if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lLen(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Long] =
    async(_.lLen(key))

  /**
   * Removes and returns the first element of a list.
   *
   * @param key list key
   * @return the popped element, or $none if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lPop[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.lPop(key))

  /**
   * Removes and returns the last element of a list.
   *
   * @param key list key
   * @return the popped element, or $none if the key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def rPop[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.rPop(key))

  /**
   * Prepends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param key list key
   * @param value value to prepend
   * @param values additional values to prepend (only works with Redis >= 2.4)
   * @return the length of the list after the push operations
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lPush(key: String, value: Any, values: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.lPush(key, value, values: _*))

  /**
   * Appends one or multiple values to a list.
   *
   * @note If key does not exist, it is created as empty list before performing the push operation.
   * Redis versions older than 2.4 can only push one value per call.
   *
   * @param key list key
   * @param value value to prepend
   * @param values additional values to prepend (only works with Redis >= 2.4)
   * @return the length of the list after the push operations
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def rPush(key: String, value: Any, values: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.rPush(key, value, values: _*))

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
  def lPushX(key: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.lPushX(key, value))

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
  def rPushX(key: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.rPushX(key, value))

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
   * @param end end offset (inclusive)
   * @return list of elements in the specified range, or the empty list if there are no such
   * elements or if key does not exist
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lRange[A](key: String, start: Long = 0, end: Long = -1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[List[A]] = async(_.lRange(key, start, end))

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
  def lRem(key: String, value: Any, count: Long = 0)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.lRem(key, value, count))

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
  def lSet(key: String, index: Long, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Unit] = async(_.lSet(key, index, value))

  /**
   * Trims a list to the specified range.
   *
   * @note Out of range indexes will not produce an error: if start is larger than the end of the
   * list, or start > end, the result will be an empty list (which causes key to be removed). If
   * end is larger than the end of the list, Redis will treat it like the last element of the list.
   *
   * @param key list key
   * @param start start offset (inclusive)
   * @param end end offset (inclusive)
   * @throws $e if key contains a non-list value
   *
   * @since 1.0.0
   */
  def lTrim(key: String, start: Long, end: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Unit] = async(_.lTrim(key, start, end))

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
  def rPopLPush[A](sourceKey: String, destKey: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.rPopLPush(sourceKey, destKey))

}