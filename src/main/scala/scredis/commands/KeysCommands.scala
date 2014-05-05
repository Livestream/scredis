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
package scredis.commands

import scala.concurrent.duration._

import scredis.CommandOptions
import scredis.protocol.Protocol
import scredis.parsing._
import scredis.parsing.Implicits._

import scala.collection.mutable.ListBuffer

/**
 * This trait implements keys commands.
 *
 * @define none `None`
 * @define e [[scredis.exceptions.RedisCommandException]]
 */
trait KeysCommands { self: Protocol =>
  import Names._

  protected def generateSortCommand(
    key: String,
    by: Option[String],
    limit: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean
  ): ListBuffer[Any] = {
    val command = ListBuffer[Any]()
    command += Sort += key
    by.foreach(command += "BY" += _)
    limit.foreach {
      case (offset, limit) => command += "LIMIT" += offset += limit
    }
    get.foreach(x => command += "GET" += x)
    if (desc) command += "DESC"
    if (alpha) command += "ALPHA"
    command
  }

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
  def del(key: String, keys: String*)(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send((Seq(Del, key) ++ keys): _*)(asInteger)

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
  ): Option[Array[Byte]] = send(Dump, key)(asBulk[Array[Byte]])

  /**
   * Determines if a key exists.
   *
   * @param key key to check for existence
   * @return true if the key exists, false otherwise
   *
   * @since 1.0.0
   */
  def exists(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Boolean =
    send(Exists, key)(asInteger(toBoolean))

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
  ): Boolean = send(Expire, key, ttlSeconds)(asInteger(toBoolean))

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
  ): Boolean = send(PExpire, key, ttlMillis)(asInteger(toBoolean))

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
  ): Boolean = send(PExpire, key, ttl.toMillis)(asInteger(toBoolean))

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
  ): Boolean = send(ExpireAt, key, timestamp)(asInteger(toBoolean))

  /**
   * Sets the expiration for a key as a UNIX timestamp specified in milliseconds.
   *
   * @param  key key to expire
   * @param timestampMillis  UNIX milliseconds-timestamp at which the key should expire
   * @return true if the ttl was set, false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpireAt(key: String, timestampMillis: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(PExpireAt, key, timestampMillis)(asInteger(toBoolean))

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
  ): Set[String] = send(Keys, pattern)(
    asMultiBulk[String, String, Set](asBulk[String, String](flatten))
  )

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
    timeout: FiniteDuration = if (connection.timeout.isFinite) {
      connection.timeout.asInstanceOf[FiniteDuration]
    } else {
      2 seconds
    },
    copy: Boolean = false,
    replace: Boolean = false
  )(implicit opts: CommandOptions = DefaultCommandOptions): Unit = {
    val command = ListBuffer[Any](Migrate, host, port, key, database, timeout.toMillis)
    if(copy) command += "COPY"
    if(replace) command += "REPLACE"
    send(command: _*)(asOkStatus)
  }

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
  ): Boolean = send(Move, key, database)(asInteger(toBoolean))

  /**
   * Removes the expiration from a key.
   *
   * @param key key to persist
   * @return true if key was persisted, false if key does not exist or does not have an
   * associated timeout
   *
   * @since 2.2.0
   */
  def persist(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Boolean =
    send(Persist, key)(asInteger(toBoolean))

  /**
   * Returns a random key from the keyspace.
   *
   * @return the random key or $none when the database is empty
   * associated timeout
   *
   * @since 1.0.0
   */
  def randomKey()(implicit opts: CommandOptions = DefaultCommandOptions): Option[String] =
    send(RandomKey)(asBulk[String])

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
  ): Unit = send(Rename, key, newKey)(asUnit)

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
  ): Boolean = send(RenameNX, key, newKey)(asInteger(toBoolean))

  /**
   * Creates a key using the provided serialized value, previously obtained using DUMP.
   *
   * @param key destination key
   * @param serializedValue serialized value, previously obtained using DUMP
   * @param ttl optional time-to-live duration of newly created key (expire)
   * @throws $e if the value could not be restored
   *
   * @since 2.6.0
   */
  def restore(key: String, serializedValue: Array[Byte], ttl: Option[FiniteDuration] = None)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = send(Restore, key, ttl.map(_.toMillis).getOrElse(0), serializedValue)(asUnit)

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
  ): List[Option[A]] = {
    send(generateSortCommand(key, by, limit, get, desc, alpha): _*)(asMultiBulk[A, List])
  }

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
  )(implicit opts: CommandOptions = DefaultCommandOptions): Long = {
    val command = generateSortCommand(key, by, limit, get, desc, alpha)
    command += "STORE" += targetKey
    send(command: _*)(asInteger)
  }

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
  ): Either[Boolean, Int] = send(Ttl, key)(asInteger { x =>
    if (x == -2) {
      Left(false)
    } else if (x == -1) {
      Left(true)
    } else {
      Right(x.toInt)
    }
  })

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
  ): Either[Boolean, Long] = send(PTtl, key)(asInteger { x =>
    if (x == -2) {
      Left(false)
    } else if (x == -1) {
      Left(true)
    } else {
      Right(x)
    }
  })

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
  ): Either[Boolean, FiniteDuration] = send(PTtl, key)(asInteger { x =>
    if (x == -2) {
      Left(false)
    } else if (x == -1) {
      Left(true)
    } else {
      Right(x milliseconds)
    }
  })

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
  def `type`(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Option[String] =
    send(Type, key)(asStatus(x => if (x.toLowerCase() == "none") None else Some(x)))
  
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
  ): (Long, Set[A]) = send(
    generateScanLikeArgs(Scan, None, cursor, countOpt, matchOpt): _*
  )(asScanMultiBulk[A, Set])
  
}