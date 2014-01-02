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

import akka.dispatch.Future
import akka.util.{ Duration, FiniteDuration }
import akka.util.duration._

import scredis.CommandOptions
import scredis.parsing._

/**
 * This trait implements asynchronous keys commands.
 * 
 * @define none `None`
 * @define e [[scredis.exceptions.RedisCommandException]]
 */
trait KeysCommands extends Async {

  /**
   * Deletes one or multiple keys.
   *
   * @note a key is ignored if it does not exist
   *
   * @param key key to delete
   * @param keys additional keys to delete
   * @return the number of keys that were deleted
   *
   * @since 1.0.0
   */
  def del(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Long] = async(_.del(key, keys: _*))

  /**
   * Returns a serialized version of the value stored at the specified key.
   *
   * @param key key to dump
   * @return the serialized value or $none if the key does not exist
   *
   * @since 2.6.0
   */
  def dump(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Option[Array[Byte]]] = async(_.dump(key))

  /**
   * Determines if a key exists.
   *
   * @param key key to check for existence
   * @return true if the key exists, false otherwise
   *
   * @since 1.0.0
   */
  def exists(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Boolean] =
    async(_.exists(key))

  /**
   * Sets a key's time to live in seconds.
   *
   * @param key key to expire
   * @param ttlSeconds time-to-live in seconds
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.0.0
   */
  def expire(key: String, ttlSeconds: Int)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.expire(key, ttlSeconds))

  /**
   * Sets a key's time to live in milliseconds.
   *
   * @param key key to expire
   * @param ttlMillis time-to-live in milliseconds
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpire(key: String, ttlMillis: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.pExpire(key, ttlMillis))

  /**
   * Sets a key's time to live.
   *
   * @param key key to expire
   * @param ttl duration after which the key should expire, up to milliseconds precision
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def expireFromDuration(key: String, ttl: FiniteDuration)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.expireFromDuration(key, ttl))

  /**
   * Sets the expiration for a key as a UNIX timestamp.
   *
   * @param key key to expire
   * @param timestamp  UNIX timestamp at which the key should expire
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.2.0
   */
  def expireAt(key: String, timestamp: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.expireAt(key, timestamp))

  /**
   * Sets the expiration for a key as a UNIX timestamp specified in milliseconds.
   *
   * @param key key to expire
   * @param timestampMillis  UNIX milliseconds-timestamp at which the key should expire
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpireAt(key: String, timestampMillis: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.pExpireAt(key, timestampMillis))

  /**
   * Finds all keys matching the given pattern.
   *
   * @param pattern pattern to search for
   * @return the matched keys, or the empty set if no keys match the given pattern
   *
   * @since 1.0.0
   */
  def keys(pattern: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Set[String]] = async(_.keys(pattern))

  /**
   * Atomically transfers a key from a Redis instance to another one.
   *
   * @param key key to transfer
   * @param host destination host
   * @param port destination port
   * @param database destination database
   * @param timeout timeout duration, up to milliseconds precision
   * @param copy if true, do not remove the key from the local instance
   * @param replace if true, replace existing key on the remote instance
   * @throws $e if an error occurs
   *
   * @since 2.6.0
   */
  def migrate(
    key: String,
    host: String,
    port: Int = 6379,
    database: Int = 0,
    timeout: FiniteDuration = 2 seconds,
    copy: Boolean = false,
    replace: Boolean = false
  )(implicit opts: CommandOptions = DefaultCommandOptions): Future[Unit] =
    async(_.migrate(key, host, port, database, timeout, copy, replace))

  /**
   * Moves a key to another database.
   *
   * @param key key to move
   * @param database destination database
   * @return true if key was moved, false otherwise
   *
   * @since 1.0.0
   */
  def move(key: String, database: Int)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.move(key, database))

  /**
   * Removes the expiration from a key.
   *
   * @param key key to persist
   * @return true if key was persisted, false if key does not exist or does not have an
   * associated timeout
   *
   * @since 2.2.0
   */
  def persist(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Boolean] =
    async(_.persist(key))

  /**
   * Returns a random key from the keyspace.
   *
   * @return the random key or $none when the database is empty
   * associated timeout
   *
   * @since 1.0.0
   */
  def randomKey()(implicit opts: CommandOptions = DefaultCommandOptions): Future[Option[String]] =
    async(_.randomKey())

  /**
   * Renames a key.
   *
   * @note if newKey already exists, it is overwritten
   * @param key source key
   * @param newKey destination key
   * @throws $e if the source and destination keys are the same, or when key
   * does not exist
   *
   * @since 1.0.0
   */
  def rename(key: String, newKey: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Unit] = async(_.rename(key, newKey))

  /**
   * Renames a key, only if the new key does not exist.
   *
   * @param key source key
   * @param newKey destination key
   * @return true if key was renamed to newKey, false if newKey already exists
   * @throws $e if the source and destination keys are the same, or when key does not exist
   *
   * @since 1.0.0
   */
  def renameNX(key: String, newKey: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Boolean] = async(_.renameNX(key, newKey))

  /**
   * Creates a key using the provided serialized value, previously obtained using DUMP.
   *
   * @param key destination key
   * @param serializedValue serialized value, previously obtained using DUMP
   * @param ttl optional time-to-live duration of newly created key (expire)
   * @throws $e  if the value could not be restored
   *
   * @since 2.6.0
   */
  def restore(key: String, serializedValue: Array[Byte], ttl: Option[FiniteDuration] = None)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Unit] = async(_.restore(key, serializedValue, ttl))

  /**
   * Sorts the elements of a list, set or sorted set.
   *
   * @param key key of list, set or sorted set to be sorted
   * @param by optional pattern for sorting by external values, can also be "nosort" if
   * the sorting operation should be skipped (useful when only sorting to retrieve objects
   * with get). The * gets replaced by the values of the elements in the collection
   * @param limit optional pair of numbers (offset, count) where offset specified the number of
   * elements to skip and count specifies the number of elements to return starting from offset
   * @param get list of patterns for retrieving objects stored in external keys. The * gets
   * replaced by the values of the elements in the collection
   * @param desc indicates whether elements should be sorted descendingly
   * @param alpha indicates whether elements should be sorted lexicographically
   * @return the sorted list of elements, or the empty list if the key does not exist
   * @throws $e whenever an error occurs
   *
   * @since 1.0.0
   */
  def sort[A](
    key: String,
    by: Option[String] = None,
    limit: Option[(Long, Long)] = None,
    get: Traversable[String] = Traversable(),
    desc: Boolean = false,
    alpha: Boolean = false
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[List[Option[A]]] = async(_.sort(key, by, limit, get, desc, alpha))

  /**
   * Sorts the elements of a list, set or sorted set and then store the result.
   *
   * @param key key of list, set or sorted set to be sorted
   * @param targetKey key of list, set or sorted set to be sorted
   * @param by optional pattern for sorting by external values, can also be "nosort" if
   * the sorting operation should be skipped (useful when only sorting to retrieve objects
   * with get). The * gets replaced by the values of the elements in the collection
   * @param limit optional pair of numbers (offset, count) where offset specified the
   * number of elements to skip and count specifies the number of elements to return starting
   * from offset
   * @param get list of patterns for retrieving objects stored in external keys. The * gets
   * replaced by the values of the elements in the collection
   * @param desc indicates whether elements should be sorted descendingly
   * @param alpha indicates whether elements should be sorted lexicographically
   * @return the number of elements in the newly stored sorted collection
   * @throws $e whenever an error occurs
   *
   * @since 1.0.0
   */
  def sortAndStore(
    key: String,
    targetKey: String,
    by: Option[String] = None,
    limit: Option[(Long, Long)] = None,
    get: Traversable[String] = Traversable(),
    desc: Boolean = false,
    alpha: Boolean = false
  )(implicit opts: CommandOptions = DefaultCommandOptions): Future[Long] =
    async(_.sortAndStore(key, targetKey, by, limit, get, desc, alpha))

  /**
   * Gets the time to live for a key in seconds.
   * 
   * {{{
   * result match {
   *   case Left(false) => // key does not exist
   *   case Left(true) => // key exists but has no associated expire
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @note For `Redis` version <= 2.8.x, `Left(false)` will be returned when the key does not
   * exists and when it exists but has no associated expire (`Redis` returns the same error code
   * for both cases). In other words, you can simply check the following
   * 
   * {{{
   * result match {
   *   case Left(_) =>
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @param key the target key
   * @return `Right(ttl)` where ttl is the time-to-live in seconds for specified key,
   * `Left(false)` if key does not exist or `Left(true)` if key exists but has no associated
   * expire
   *
   * @since 1.0.0
   */
  def ttl(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Either[Boolean, Int]] = async(_.ttl(key))

  /**
   * Gets the time to live for a key in milliseconds.
   * 
   * {{{
   * result match {
   *   case Left(false) => // key does not exist
   *   case Left(true) => // key exists but has no associated expire
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @note For `Redis` version <= 2.8.x, `Left(false)` will be returned when the key does not
   * exists and when it exists but has no associated expire (`Redis` returns the same error code
   * for both cases). In other words, you can simply check the following
   * 
   * {{{
   * result match {
   *   case Left(_) =>
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @param key the target key
   * @return `Right(ttl)` where ttl is the time-to-live in milliseconds for specified key,
   * `Left(false)` if key does not exist or `Left(true)` if key exists but has no associated
   * expire
   *
   * @since 2.6.0
   */
  def pTtl(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Either[Boolean, Long]] = async(_.pTtl(key))

  /**
   * Gets the time to live `FiniteDuration` for a key.
   * 
   * {{{
   * result match {
   *   case Left(false) => // key does not exist
   *   case Left(true) => // key exists but has no associated expire
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @note For `Redis` version <= 2.8.x, `Left(false)` will be returned when the key does not
   * exists and when it exists but has no associated expire (`Redis` returns the same error code
   * for both cases). In other words, you can simply check the following
   * 
   * {{{
   * result match {
   *   case Left(_) =>
   *   case Right(ttl) =>
   * }
   * }}}
   * 
   * @param key the target key
   * @return `Right(ttl)` where ttl is the time-to-live `FiniteDuration` for specified key,
   * `Left(false)` if key does not exist or `Left(true)` if key exists but has no associated expire
   *
   * @since 2.6.0
   */
  def ttlDuration(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Either[Boolean, FiniteDuration]] = async(_.ttlDuration(key))

  /**
   * Determine the type stored at key.
   *
   * @note This method needs to be called as follows:
   * {{{
   * client.`type`(key)
   * }}}
   *
   * @param key key for which the type should be returned
   * @return type of key, or $none if key does not exist
   *
   * @since 1.0.0
   */
  def `type`(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Option[String]] = async(_.`type`(key))
  
  /**
   * Incrementally iterates the set of keys in the currently selected Redis database.
   *
   * @param cursor the offset
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @return a pair containing the next cursor as its first element and the set of keys
   * as its second element
   *
   * @since 2.8.0
   */
  def scan[A](cursor: Long, countOpt: Option[Int] = None, matchOpt: Option[String] = None)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[(Long, Set[A])] = async(_.scan(cursor, countOpt, matchOpt))
  
}