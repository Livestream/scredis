package scredis.commands

import scredis.AbstractClient
import scredis.protocol.requests.HashRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future

/**
 * This trait implements hash commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait HashCommands { self: AbstractClient =>
  
  /**
   * Deletes one or more hash fields.
   *
   * @note Specified fields that do not exist within this hash are ignored. If key does not exist,
   * it is treated as an empty hash and this command returns 0. Redis versions older than 2.4 can
   * only remove a field per call.
   *
   * @param key key of the hash
   * @param fields field(s) to be deleted from hash
   * @return the number of fields that were removed from the hash, not including specified but non
   * existing fields
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hDel(key: String, fields: String*): Future[Long] = send(HDel(key, fields: _*))
  
  /**
   * Determines if a hash field exists.
   *
   * @param key hash key
   * @param field name of the field
   * @return $true if the hash contains field, $false if the hash does not contain it or
   * the key does not exists
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hExists(key: String, field: String): Future[Boolean] = send(HExists(key, field))
  
  /**
   * Returns the value of a hash field.
   *
   * @param key hash key
   * @param field field name to retrieve
   * @return the value associated with field name, or $none when field is not present in the hash
   * or key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hGet[R: Reader](key: String, field: String): Future[Option[R]] = send(HGet(key, field))
  
  /**
   * Returns all the fields and values in a hash.
   *
   * @param key hash key
   * @return key-value pairs stored in hash with key, or $none when hash is empty or key does not
   * exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hGetAll[R: Reader](key: String): Future[Option[Map[String, R]]] = send(HGetAll(key)).map {
    data => if (data.isEmpty) {
      None
    } else {
      Some(data)
    }
  }
  
  /**
   * Increments the integer value of a hash field by the given number.
   *
   * @note If key does not exist, a new key holding a hash is created. If field does not exist the
   * value is set to 0 before the operation is performed.
   *
   * @param key hash key
   * @param field field name to increment
   * @param count increment
   * @return the value at field after the increment operation
   * @throws $e if the field does not hold an integer value or if the value stored at key is not of
   * type hash
   *
   * @since 2.0.0
   */
  def hIncrBy(key: String, field: String, count: Long): Future[Long] = send(
    HIncrBy(key, field, count)
  )
  
  /**
   * Increments the float value of a hash field by the given amount.
   *
   * @note If key does not exist, a new key holding a hash is created. If field does not exist the
   * value is set to 0 before the operation is performed.
   *
   * @param key hash key
   * @param field field name to increment
   * @param count increment
   * @return the value at field after the increment operation
   * @throws $e if the field does not hold a floating point value or if the value stored at key is
   * not of type hash
   *
   * @since 2.6.0
   */
  def hIncrByFloat(key: String, field: String, count: Double): Future[Double] = send(
    HIncrByFloat(key, field, count)
  )
  
  /**
   * Returns all the fields in a hash.
   *
   * @param key hash key
   * @return set of field names or the empty set if the hash is empty or the key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hKeys(key: String): Future[Set[String]] = send(HKeys[Set](key))
  
  /**
   * Returns the number of fields contained in the hash stored at key.
   *
   * @param key hash key
   * @return number of fields in the hash, or 0 if the key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hLen(key: String): Future[Long] = send(HLen(key))
  
  /**
   * Returns the values associated to the specified hash fields.
   *
   * @note For every field that does not exist, $none is returned.
   *
   * @param key hash key
   * @param fields field(s) to retrieve
   * @return list of value(s) associated to the specified field name(s)
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmGet[R: Reader](key: String, fields: String*): Future[List[Option[R]]] = send(
    HMGet[R, List](key, fields: _*)
  )
  
  /**
   * Returns a `Map` containing field-value pairs associated to the specified hash fields.
   *
   * @note Every non-existent field gets removed from the resulting `Map`.
   *
   * @param key hash key
   * @param fields field(s) to retrieve
   * @return field-value pairs associated to the specified field name(s)
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmGetAsMap[R: Reader](key: String, fields: String*): Future[Map[String, R]] = send(
    HMGetAsMap(key, fields: _*)
  )
  
  /**
   * Sets multiple hash fields to multiple values.
   *
   * @note This command overwrites any existing fields in the hash. If key does not exist, a new
   * key holding a hash is created
   *
   * @param key hash key
   * @param fieldValuePairs field-value pair(s) to be set
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmSet[W: Writer](key: String, fieldValuePairs: Map[String, W]): Future[Unit] = send(
    HMSet(key, fieldValuePairs.toList: _*)
  )
  
  /**
   * Incrementally iterates through the fields of a hash.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the list of fields
   * (key-value pairs) as its second element
   *
   * @since 2.8.0
   */
  def hScan[R: Reader](
    key: String,
    cursor: Long,
    matchOpt: Option[String] = None,
    countOpt: Option[Int] = None
  ): Future[(Long, List[(String, R)])] = send(
    HScan[R, List](
      key = key,
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    )
  )
  
  /**
   * Sets the string value of a hash field.
   *
   * @note If the field already exists in the hash, it is overwritten.
   *
   * @param key hash key
   * @param field field name to set
   * @param value value to set
   * @return $true if field is a new field in the hash and value was set, $false if
   * field already exists and the value was updated
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hSet[W: Writer](key: String, field: String, value: W): Future[Boolean] = send(
    HSet(key, field, value)
  )
  
  /**
   * Sets the value of a hash field, only if the field does not exist.
   *
   * @param key hash key
   * @param field field name to set
   * @param value value to set
   * @return $true if field is a new field in the hash and value was set, $false if
   * field already exists and no operation was performed
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hSetNX[W: Writer](key: String, field: String, value: W): Future[Boolean] = send(
    HSetNX(key, field, value)
  )

  /**
   * Returns all the values in a hash.
   *
   * @param key hash key
   * @return list of values, or the empty list if hash is empty or key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hVals[R: Reader](key: String): Future[List[R]] = send(HVals[R, List](key))
  
}