package scredis.commands

import scala.language.postfixOps
import scredis.io.NonBlockingConnection
import scredis.protocol.requests.KeyRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements key commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait KeyCommands { self: NonBlockingConnection =>
  
  /**
   * Deletes one or multiple keys.
   *
   * @note a key is ignored if it does not exist
   *
   * @param keys key(s) to delete
   * @return the number of keys that were deleted
   *
   * @since 1.0.0
   */
  def del(keys: String*): Future[Long] = send(Del(keys: _*))
  
  /**
   * Returns a serialized version of the value stored at the specified key.
   *
   * @param key key to dump
   * @return the serialized value or $none if the key does not exist
   *
   * @since 2.6.0
   */
  def dump(key: String): Future[Option[Array[Byte]]] = send(Dump(key))
  
  /**
   * Determines if a key exists.
   *
   * @param key key to check for existence
   * @return $true if the key exists, $false otherwise
   *
   * @since 1.0.0
   */
  def exists(key: String): Future[Boolean] = send(Exists(key))
  
  /**
   * Sets a key's time to live in seconds.
   *
   * @param key key to expire
   * @param ttlSeconds time-to-live in seconds
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.0.0
   */
  def expire(key: String, ttlSeconds: Int): Future[Boolean] = send(Expire(key, ttlSeconds))

  /**
   * Sets the expiration for a key as a UNIX timestamp.
   *
   * @param key key to expire
   * @param timestamp  UNIX timestamp at which the key should expire
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 1.2.0
   */
  def expireAt(key: String, timestamp: Long): Future[Boolean] = send(ExpireAt(key, timestamp))
  
  /**
   * Finds all keys matching the given pattern.
   *
   * @param pattern pattern to search for
   * @return the matched keys, or the empty set if no keys match the given pattern
   *
   * @since 1.0.0
   */
  def keys(pattern: String): Future[Set[String]] = send(Keys[Set](pattern))
  
  /**
   * Atomically transfers a key from a Redis instance to another one.
   *
   * @param key key to transfer
   * @param host destination host
   * @param port destination port
   * @param database destination database
   * @param timeout timeout duration, up to milliseconds precision
   * @param copy if $true, do not remove the key from the local instance
   * @param replace if $true, replace existing key on the remote instance
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
  ): Future[Unit] = send(
    Migrate(
      key = key,
      host = host,
      port = port,
      database = database,
      timeout = timeout,
      copy = copy,
      replace = replace
    )
  )
  
  /**
   * Moves a key to another database.
   *
   * @param key key to move
   * @param database destination database
   * @return true if key was moved, false otherwise
   *
   * @since 1.0.0
   */
  def move(key: String, database: Int): Future[Boolean] = send(Move(key, database))
  
  /**
   * Returns the number of references of the value associated with the specified key.
   *
   * @param key the key
   * @return the number of references or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectRefCount(key: String): Future[Option[Long]] = send(ObjectRefCount(key))
  
  /**
   * Returns the kind of internal representation used in order to store the value associated with
   * a key.
   * 
   * @note Objects can be encoded in different ways:
   * Strings can be encoded as `raw` or `int`
   * Lists can be encoded as `ziplist` or `linkedlist`
   * Sets can be encoded as `intset` or `hashtable`
   * Hashes can be encoded as `zipmap` or `hashtable`
   * SortedSets can be encoded as `ziplist` or `skiplist`
   *
   * @param key the key
   * @return the object encoding or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectEncoding(key: String): Future[Option[String]] = send(ObjectEncoding(key))
  
  /**
   * Returns the number of seconds since the object stored at the specified key is idle (not
   * requested by read or write operations).
   * 
   * @note While the value is returned in seconds the actual resolution of this timer is
   * 10 seconds, but may vary in future implementations.
   *
   * @param key the key
   * @return the number of seconds since the object is idle or $none if the key does not exist
   *
   * @since 2.2.3
   */
  def objectIdleTime(key: String): Future[Option[Long]] = send(ObjectIdleTime(key))
  
  /**
   * Removes the expiration from a key.
   *
   * @param key key to persist
   * @return $true if key was persisted, $false if key does not exist or does not have an
   * associated timeout
   *
   * @since 2.2.0
   */
  def persist(key: String): Future[Boolean] = send(Persist(key))
  
  /**
   * Sets a key's time to live in milliseconds.
   *
   * @param key key to expire
   * @param ttlMillis time-to-live in milliseconds
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpire(key: String, ttlMillis: Long): Future[Boolean] = send(PExpire(key, ttlMillis))
  
  /**
   * Sets the expiration for a key as a UNIX timestamp specified in milliseconds.
   *
   * @param key key to expire
   * @param timestampMillis  UNIX milliseconds-timestamp at which the key should expire
   * @return $true if the ttl was set, $false if key does not exist or
   * the timeout could not be set
   *
   * @since 2.6.0
   */
  def pExpireAt(key: String, timestampMillis: Long): Future[Boolean] = send(
    PExpireAt(key, timestampMillis)
  )
  
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
  def pTtl(key: String): Future[Either[Boolean, Long]] = send(PTTL(key))
  
  /**
   * Returns a random key from the keyspace.
   *
   * @return the random key or $none when the database is empty
   * associated timeout
   *
   * @since 1.0.0
   */
  def randomKey(): Future[Option[String]] = send(RandomKey())
  
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
  def rename(key: String, newKey: String): Future[Unit] = send(Rename(key, newKey))
  
  /**
   * Renames a key, only if the new key does not exist.
   *
   * @param key source key
   * @param newKey destination key
   * @return $true if key was renamed to newKey, $false if newKey already exists
   * @throws $e if the source and destination keys are the same, or when key does not exist
   *
   * @since 1.0.0
   */
  def renameNX(key: String, newKey: String): Future[Boolean] = send(RenameNX(key, newKey))
  
  /**
   * Creates a key using the provided serialized value, previously obtained using DUMP.
   *
   * @param key destination key
   * @param serializedValue serialized value, previously obtained using DUMP
   * @param ttlOpt optional time-to-live duration of newly created key (expire)
   * @throws $e if the value could not be restored
   *
   * @since 2.6.0
   */
  def restore[W: Writer](
    key: String,
    serializedValue: W,
    ttlOpt: Option[FiniteDuration] = None
  ): Future[Unit] = send(
    Restore(
      key = key,
      value = serializedValue,
      ttlOpt = ttlOpt
    )
  )
  
  /**
   * Incrementally iterates the set of keys in the currently selected Redis database.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the set of keys
   * as its second element
   *
   * @since 2.8.0
   */
  def scan(
    cursor: Long,
    matchOpt: Option[String] = None,
    countOpt: Option[Int] = None
  ): Future[(Long, Set[String])] = send(
    Scan[Set](
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    )
  )
  
  /**
   * Sorts the elements of a list, set or sorted set.
   *
   * @param key key of list, set or sorted set to be sorted
   * @param byOpt optional pattern for sorting by external values, can also be "nosort" if
   * the sorting operation should be skipped (useful when only sorting to retrieve objects
   * with get). The * gets replaced by the values of the elements in the collection
   * @param limitOpt optional pair of numbers (offset, count) where offset specified the number of
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
  def sort[R: Reader](
    key: String,
    byOpt: Option[String] = None,
    limitOpt: Option[(Long, Long)] = None,
    get: Traversable[String] = Nil,
    desc: Boolean = false,
    alpha: Boolean = false
  ): Future[List[Option[R]]] = send(
    Sort[R, List](
      key = key,
      byOpt = byOpt,
      limitOpt = limitOpt,
      get = get,
      desc = desc,
      alpha = alpha
    )
  )

  /**
   * Sorts the elements of a list, set or sorted set and then store the result.
   *
   * @param key key of list, set or sorted set to be sorted
   * @param targetKey key of list, set or sorted set to be sorted
   * @param byOpt optional pattern for sorting by external values, can also be "nosort" if
   * the sorting operation should be skipped (useful when only sorting to retrieve objects
   * with get). The * gets replaced by the values of the elements in the collection
   * @param limitOpt optional pair of numbers (offset, count) where offset specified the
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
    byOpt: Option[String] = None,
    limitOpt: Option[(Long, Long)] = None,
    get: Traversable[String] = Nil,
    desc: Boolean = false,
    alpha: Boolean = false
  ): Future[Long] = send(
    SortAndStore(
      key = key,
      targetKey = targetKey,
      byOpt = byOpt,
      limitOpt = limitOpt,
      get = get,
      desc = desc,
      alpha = alpha
    )
  )
  
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
  def ttl(key: String): Future[Either[Boolean, Int]] = send(TTL(key))
  
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
  def `type`(key: String): Future[Option[scredis.Type]] = send(Type(key))
  
}
