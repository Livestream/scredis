package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.StringRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements string commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait StringCommands { self: NonBlockingConnection =>
  
  /**
   * Appends a value to a key.
   *
   * @param key the key to be appended
   * @param value the value to append
   * @return the length of the string after the append operation
   * @throws $e if the value stored at key is not of type string
   *
   * @since 2.0.0
   */
  def append[W: Writer](key: String, value: W): Future[Long] = send(Append(key, value))
  
  /**
   * Counts the number of bits set to 1 in a string from start offset to stop offset.
   *
   * @note Non-existent keys are treated as empty strings, so the command will return zero.
   *
   * @param key the key for which the bitcount should be returned
   * @param start start offset (defaults to 0)
   * @param stop stop offset (defaults to -1)
   * @return the number of bits set to 1 in the specified interval
   * @throws $e if the value stored at key is not of type string
   *
   * @since 2.6.0
   */
  def bitCount(key: String, start: Long = 0, stop: Long = -1): Future[Long] = send(
    BitCount(key, start, stop)
  )
  
  /**
   * Performs a bitwise operation between multiple strings.
   *
   * @note When an operation is performed between strings having different lengths, all the strings
   * shorter than the longest string in the set are treated as if they were zero-padded up to the
   * length of the longest string. The same holds true for non-existent keys, that are considered
   * as a stream of zero bytes up to the length of the longest string.
   *
   * @param operation [[scredis.BitOp]] to perform
   * @param destKey	destination key where the result of the operation will be stored
   * @param keys keys to perform the operation upon
   * @return the size of the string stored in the destination key, that is equal to the size of
   * the longest input string
   * @throws $e if at least one of the values stored at keys is not of type string
   *
   * @since 2.6.0
   */
  def bitOp(operation: scredis.BitOp, destKey: String, keys: String*): Future[Long] = send(
    BitOp(operation, destKey, keys: _*)
  )
  
  /**
   * Return the position of the first bit set to 1 or 0 in a string.
   *
   * @note The position is returned thinking at the string as an array of bits from left to right
   * where the first byte most significant bit is at position 0, the second byte most significant
   * big is at position 8 and so forth.
   * 
   * The range is interpreted as a range of bytes and not a range of bits, so start=0 and end=2
   * means to look at the first three bytes.
   * 
   * If we look for set bits (the bit argument is 1) and the string is empty or composed of just
   * zero bytes, -1 is returned.
   * 
   * If we look for clear bits (the bit argument is 0) and the string only contains bit set to 1,
   * the function returns the first bit not part of the string on the right. So if the string is
   * tree bytes set to the value 0xff the command BITPOS key 0 will return 24, since up to bit 23
   * all the bits are 1.
   * 
   * Basically the function consider the right of the string as padded with zeros if you look for
   * clear bits and specify no range or the start argument '''only'''.
   * 
   * However this behavior changes if you are looking for clear bits and specify a range with both
   * start and stop. If no clear bit is found in the specified range, the function returns -1 as
   * the user specified a clear range and there are no 0 bits in that range.
   *
   * @param key string key
   * @param bit provide $true to look for 1s and $false to look for 0s
   * @param start start offset, in bytes
   * @param stop stop offset, in bytes
   * @return the position of the first bit set to 1 or 0, according to the request
   * @throws $e if key is not of type string
   *
   * @since 2.8.7
   */
  def bitPos(key: String, bit: Boolean, start: Long = 0, stop: Long = -1): Future[Long] = send(
    BitPos(key, bit, start, stop)
  )
  
  /**
   * Decrements the integer value of a key by one.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param key the key to decrement
   * @return the value of key after the decrement
   * @throws $e if the key contains a value of the wrong type or contains a string that cannot be
   * represented as integer
   *
   * @since 1.0.0
   */
  def decr(key: String): Future[Long] = send(Decr(key))
  
  /**
   * Decrements the integer value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param key the key to decrement
   * @param decrement the decrement
   * @return the value of key after the decrement
   * @throws $e if the key contains a value of the wrong type or contains
   * a string that cannot be represented as integer
   *
   * @since 1.0.0
   */
  def decrBy(key: String, decrement: Long): Future[Long] = send(DecrBy(key, decrement))
  
  /**
   * Returns the value stored at key.
   *
   * @param key the target key
   * @return value stored at key, or $none if the key does not exist
   * @throws $e if the value stored at key is not of type string
   *
   * @since 1.0.0
   */
  def get[R: Reader](key: String): Future[Option[R]] = send(Get(key))
  
  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key the target key
   * @param offset the position in the string
   * @return $true if the bit is set to 1, $false otherwise
   * @throws $e if the value stored at key is not of type string
   *
   * @since 2.2.0
   */
  def getBit(key: String, offset: Long): Future[Boolean] = send(GetBit(key, offset))
  
  /**
   * Returns a substring of the string stored at a key.
   *
   * @note Both offsets are inclusive, i.e. [start, stop]. The function handles out of range
   * requests by limiting the resulting range to the actual length of the string.
   *
   * @param key the target key
   * @param start the start offset (inclusive)
   * @param stop the stop offset (inclusive)
   * @return the substring determined by the specified offsets, or the empty string if the key
   * does not exist
   * @throws $e if the value stored at key is not of type string
   *
   * @since 2.4.0
   */
  def getRange[R: Reader](key: String, start: Long, stop: Long): Future[R] = send(
    GetRange(key, start, stop)
  )
  
  /**
   * Sets the string value of a key and return its old value.
   *
   * @param key the target key
   * @param value the value to set key to
   * @return the old value, or $none if the latter did not exist
   * @throws $e if the value stored at key is not of type string
   *
   * @since 1.0.0
   */
  def getSet[R: Reader, W: Writer](key: String, value: W): Future[Option[R]] = send(
    GetSet(key, value)
  )
  
  /**
   * Increments the integer value of a key by one.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param key the key to increment
   * @return the value of key after the increment
   * @throws $e if the key contains a value of the wrong type or contains a string that cannot be
   * represented as integer
   *
   * @since 1.0.0
   */
  def incr(key: String): Future[Long] = send(Incr(key))
  
  /**
   * Increments the integer value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param key the key to increment
   * @param increment the increment
   * @return the value of key after the decrement
   * @throws $e if the key contains a value of the wrong type or contains
   * a string that cannot be represented as integer
   *
   * @since 1.0.0
   */
  def incrBy(key: String, increment: Long): Future[Long] = send(IncrBy(key, increment))
  
  /**
   * Increment the float value of a key by the given amount.
   *
   * @note If the key does not exist, it is set to 0 before performing the operation.
   *
   * @param key the key to increment
   * @param increment the increment
   * @return the value of key after the decrement
   * @throws $e if the key contains a value of the wrong type, the current key content or the
   * specified increment are not parseable as a double precision floating point number
   *
   * @since 2.6.0
   */
  def incrByFloat(key: String, increment: Double): Future[Double] = send(
    IncrByFloat(key, increment)
  )
  
  /**
   * Returns the values of all specified keys.
   *
   * @note For every key that does not hold a string value or does not exist, $none is returned.
   * Because of this, the operation never fails.
   *
   * @param keys the keys to fetch
   * @return list of value(s) stored at the specified key(s)
   *
   * @since 1.0.0
   */
  def mGet[R: Reader](keys: String*): Future[List[Option[R]]] = send(MGet[R, List](keys: _*))
  
  /**
   * Returns a `Map` containing the specified key(s) paired to their respective value(s).
   *
   * @note Every key that does not hold a string value or does not exist will be removed from the
   * resulting `Map`.
   *
   * @param keys the keys to fetch
   * @return map of key-value pairs
   *
   * @since 1.0.0
   */
  def mGetAsMap[R: Reader](keys: String*): Future[Map[String, R]] = send(MGetAsMap[R](keys: _*))
  
  /**
   * Atomically sets multiple keys to multiple values.
   *
   * @note MSET replaces existing values with new values, just as regular SET.
   *
   * @param keyValuePairs map of key-value pairs to set
   *
   * @since 1.0.1
   */
  def mSet[W: Writer](keyValuePairs: Map[String, W]): Future[Unit] = {
    if (keyValuePairs.isEmpty) {
      Future.successful(())
    } else {
      send(MSet(keyValuePairs))
    }
  }
  
  /**
   * Atomically sets multiple keys to multiple values, only if none of the keys exist.
   *
   * @note MSETNX will not perform any operation at all even if just a single key already exists.
   *
   * @param keyValuePairs map of key-value pairs to set
   * @return $true if all the keys were set, $false if at least one key already existed and thus
   * no operation was performed.
   *
   * @since 1.0.1
   */
  def mSetNX[W: Writer](keyValuePairs: Map[String, W]): Future[Boolean] = send(
    MSetNX(keyValuePairs)
  )
  
  /**
   * Sets the value and expiration in milliseconds of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param key target key to set
   * @param value value to be stored at key
   * @param ttlMillis time-to-live in milliseconds
   *
   * @since 2.6.0
   */
  def pSetEX[W: Writer](key: String, value: W, ttlMillis: Long): Future[Unit] = send(
    PSetEX(key, ttlMillis, value)
  )
  
  /**
   * Sets the string value of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type. Any previous
   * time to live associated with the key is discarded on successful SET operation.
   * 
   * The ttlOpt and conditionOpt parameters can only be used with `Redis` >= 2.6.12
   * 
   * @param key target key to set
   * @param value value to be stored at key
   * @param ttlOpt optional time-to-live (up to milliseconds precision)
   * @param conditionOpt optional condition to be met for the value to be set
   * @return $true if the value was set correctly, $false if a condition was specified but not met
   *
   * @since 1.0.0
   */
  def set[W: Writer](
    key: String,
    value: W,
    ttlOpt: Option[FiniteDuration] = None,
    conditionOpt: Option[scredis.Condition] = None
  ): Future[Boolean] = send(
    Set(
      key = key,
      value = value,
      ttlOpt = ttlOpt,
      conditionOpt = conditionOpt
    )
  )
  
  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @note When key does not exist, a new string value is created. The string is grown to make sure
   * it can hold a bit at offset. When the string at key is grown, added bits are set to 0.
   *
   * @param key key for which the bit should be set
   * @param offset position where the bit should be set
   * @param bit $true sets the bit to 1, $false sets it to 0
   * @throws $e if the key contains a value of the wrong type
   *
   * @since 2.2.0
   */
  def setBit(key: String, offset: Long, bit: Boolean): Future[Boolean] = send(
    SetBit(key, offset, bit)
  )
  
  /**
   * Sets the value and expiration in seconds of a key.
   *
   * @note If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param key target key to set
   * @param value value to be stored at key
   * @param ttlSeconds time-to-live in seconds
   *
   * @since 2.0.0
   */
  def setEX[W: Writer](key: String, value: W, ttlSeconds: Int): Future[Unit] = send(
    SetEX(key, ttlSeconds, value)
  )
  
  /**
   * Sets the value of a key, only if the key does not exist.
   *
   * @param key target key to set
   * @param value value to be stored at key
   * @return $true if the key was set, $false otherwise
   *
   * @since 1.0.0
   */
  def setNX[W: Writer](key: String, value: W): Future[Boolean] = send(SetNX(key, value))
  
  /**
   * Overwrites part of a string at key starting at the specified offset.
   *
   * @note If the offset is larger than the current length of the string at key, the string is
   * padded with zero-bytes to make offset fit. Non-existing keys are considered as empty strings,
   * so this command will make sure it holds a string large enough to be able to set value at
   * offset.
   *
   * @param key target key
   * @param offset position from which the string must be overwritten
   * @param value string value to be set at given offset
   * @return the length of the string after it was modified by the command
   * @throws $e if the key contains a value of the wrong type
   *
   * @since 2.2.0
   */
  def setRange[W: Writer](key: String, offset: Long, value: W): Future[Long] = send(
    SetRange(key, offset, value)
  )
  
  /**
   * Returns the length of the string value stored in a key.
   *
   * @param key target key
   * @return the length of the string stored at key, or 0 when the key does not exist
   * @throws $e if the key contains a value of the wrong type
   *
   * @since 2.2.0
   */
  def strLen(key: String): Future[Long] = send(StrLen(key))
  
}