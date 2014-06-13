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
 * This trait implements asynchronous sets commands.
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait SetsCommands extends Async {

  /**
   * Adds one or more members to a set.
   *
   * @param key set key
   * @param member value to add
   * @param members additional values to add (only works for Redis >= 2.4)
   * @return the number of elements that were added to the set, not including all the elements
   * already present into the set
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sAdd(key: String, member: Any, members: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.sAdd(key, member, members: _*))

  /**
   * Returns the number of members in a set.
   *
   * @param key set key
   * @return the cardinality (number of elements) of the set, or 0 if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sCard(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Long] =
    async(_.sCard(key))

  /**
   * Returns the set resulting from the difference between the first set and all the successive
   * sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be substracted from first set
   * @param keys additional keys of sets to be substracted from first set
   * @return the resulting set, or the empty set if firstKey does not
   * exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiff[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Set[A]] = async(_.sDiff(firstKey, key, keys: _*))

  /**
   * Stores the set resulting from the difference between the first set and all the successive sets.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param key key of first set
   * @param keys keys of sets to be substracted from first set, if empty, first set is simply
   * copied to destKey
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiffStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.sDiffStore(destKey)(key, keys: _*))

  /**
   * Intersects multiple sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be intersected with first set
   * @param keys additional keys of sets to be intersected with all sets
   * @return the resulting set, or the empty set if firstKey does not
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInter[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Set[A]] = async(_.sInter(firstKey, key, keys: _*))

  /**
   * Intersects multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param key key of first set
   * @param keys keys of sets to be intersected with all sets, if empty, first set is simply
   * copied to destKey
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInterStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.sInterStore(destKey)(key, keys: _*))

  /**
   * Computes the union of multiple sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be intersected with first set
   * @param keys additional keys of sets to be included in the union computation
   * @return the resulting set, or the empty set if firstKey does not
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnion[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Set[A]] = async(_.sUnion(firstKey, key, keys: _*))

  /**
   * Computes the union of multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param firstKey key of first set
   * @param keys keys of sets to be included in the union computation, if empty, first set is
   * simply copied to destKey
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnionStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.sUnionStore(destKey)(key, keys: _*))

  /**
   * Determines if a given value is a member of a set.
   *
   * @param key set key
   * @param member value to be tested
   * @return true if the provided value is a member of the set stored at key.
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sIsMember(key: String, member: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.sIsMember(key, member))

  /**
   * Returns all the members of a set.
   *
   * @param key set key
   * @return set stored at key, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMembers[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Set[A]] = async(_.sMembers(key))

  /**
   * Moves a member from one set to another.
   *
   * @param sourceKey key of source set
   * @param member value to be moved from source set to destination set
   * @param deskKey key of destination set
   * @return true if the member was moved, false if the element is not a member of source set and
   * no operation was performed
   * @throws $e if sourceKey or destKey contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMove(sourceKey: String, member: Any)(destKey: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.sMove(sourceKey, member)(destKey))

  /**
   * Removes and returns a random member from a set.
   *
   * @note This operation is similar to SRANDMEMBER, that returns a random element from a set but
   * does not remove it.
   *
   * @param key set key
   * @return random member, or $none if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sPop[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.sPop(key))

  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @return random member, or $none if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRandMember[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Option[A]] = async(_.sRandMember(key))

  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @param count number of member to randomly retrieve
   * @return set of random members, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRandMembers[A](key: String, count: Int = 1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[Set[A]] = async(_.sRandMembers(key, count))

  /**
   * Removes one or more members from a set.
   *
   * @note Redis versions older than 2.4 can only remove one member per call.
   *
   * @param key set key
   * @param member value to remove from set
   * @param members additional values to remove from set (only works with Redis >= 2.4)
   * @return the number of members that were removed from the set, not including non-existing
   * members
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRem(key: String, member: Any, members: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.sRem(key, member, members: _*))
  
  /**
   * Incrementally iterates the elements of a set.
   *
   * @param cursor the offset
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @return a pair containing the next cursor as its first element and the set of elements
   * as its second element
   *
   * @since 2.8.0
   */
  def sScan[A](key: String)(
    cursor: Long, countOpt: Option[Int] = None, matchOpt: Option[String] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[(Long, Set[A])] = async(_.sScan(key)(cursor, countOpt, matchOpt))
  
}