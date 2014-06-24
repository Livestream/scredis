package scredis.commands

import scredis.CommandOptions
import scredis.protocol.Protocol
import scredis.exceptions.RedisCommandException
import scredis.parsing._
import scredis.parsing.Implicits._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

/**
 * This trait implements hashes commands.
 *
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait HashCommands { self: Protocol =>
  import Names._

  /**
   * Deletes one or more hash fields.
   *
   * @note Specified fields that do not exist within this hash are ignored. If key does not exist,
   * it is treated as an empty hash and this command returns 0. Redis versions older than 2.4 can
   * only remove a field per call.
   *
   * @param key key of the hash
   * @param field field name to be deleted from hash
   * @param fields additional fields to be deleted (only works with Redis >= 2.4)
   * @return the number of fields that were removed from the hash, not including specified but non
   * existing fields
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hDel(key: String)(field: String, fields: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(HDel, key, field) ++ fields): _*)(asInteger)

  /**
   * Determines if a hash field exists.
   *
   * @param key hash key
   * @param field name of the field
   * @return true if the hash contains field, false if the hash does not contain it or the key does
   * not exists
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hExists(key: String)(field: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(HExists, key, field)(asInteger(toBoolean))

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
  def hGet[A](key: String)(field: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Option[A] = send(HGet, key, field)(asBulk[A])

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
  def hGetAll[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Option[Map[String, A]] = send(HGetAll, key)(
    asMultiBulk[Option[Map[String, A]]](toOptionalMap[String, A])
  )

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
  def hIncrBy(key: String)(field: String, count: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(HIncrBy, key, field, count)(asInteger)

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
  def hIncrByFloat(key: String)(field: String, count: Double)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Double = send(HIncrByFloat, key, field, count)(asBulk[Double, Double](flatten))

  /**
   * Returns all the fields in a hash.
   *
   * @param key hash key
   * @return set of field names or the empty set if the hash is empty or the key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hKeys(key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Set[String] = send(HKeys, key)(
    asMultiBulk[String, String, Set](asBulk[String, String](flatten))
  )

  /**
   * Returns the number of fields contained in the hash stored at key.
   *
   * @param key hash key
   * @return number of fields in the hash, or 0 if the key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hLen(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send(HLen, key)(asInteger)

  /**
   * Returns the values associated to the specified hash fields.
   *
   * @note For every field that does not exist, $none is returned.
   *
   * @param key hash key
   * @param field field name to retrieve
   * @param fields additional field names to retrieve
   * @return list of value(s) associated to the specified field names
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmGet[A](key: String)(field: String, fields: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): List[Option[A]] = send((Seq(HMGet, key, field) ++ fields): _*)(
    asMultiBulk[A, List]
  )

  /**
   * Returns a `Map` containing field-value pairs associated to the specified hash fields.
   *
   * @note Every non-existent field gets removed from the resulting `Map`.
   *
   * @param key hash key
   * @param field field name to retrieve
   * @param fields additional field names to retrieve
   * @return field-value pairs associated to the specified field names
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmGetAsMap[A](key: String)(field: String, fields: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Map[String, A] = send((Seq(HMGet, key, field) ++ fields): _*)(
    asMultiBulk[List, Map[String, A]](toMapWithKeys[String, A](field :: fields.toList))
  )

  /**
   * Sets multiple hash fields to multiple values.
   *
   * @note This command overwrites any existing fields in the hash. If key does not exist, a new
   * key holding a hash is created
   *
   * @param key hash key
   * @param fieldValueMap field-value pairs to set
   * @throws $e if fieldValueMap is empty or the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmSetFromMap(key: String, fieldValueMap: Map[String, Any])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = if(fieldValueMap.isEmpty) {
    throw RedisCommandException("HMSET: keyValueMap cannot be empty")
  } else {
    send(flattenKeyValueMap(List(HMSet, key), fieldValueMap): _*)(asUnit)
  }

  /**
   * Sets multiple hash fields to multiple values.
   *
   * @note This command overwrites any existing fields in the hash. If key does not exist, a new
   * key holding a hash is created
   *
   * @param key hash key
   * @param fieldValuePair field-value pair to be set
   * @param fieldValuePairs additional field-value pairs to be set
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hmSet(key: String)(fieldValuePair: (String, Any), fieldValuePairs: (String, Any)*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = hmSetFromMap(key, (fieldValuePair :: fieldValuePairs.toList).toMap)

  /**
   * Sets the string value of a hash field.
   *
   * @note If the field already exists in the hash, it is overwritten.
   *
   * @param key hash key
   * @param field field name to set
   * @param value value to set
   * @return true if field is a new field in the hash and value was set, false if field already
   * exists and the value was updated
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hSet(key: String)(field: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(HSet, key, field, value)(asInteger(toBoolean))

  /**
   * Sets the value of a hash field, only if the field does not exist.
   *
   * @param key hash key
   * @param field field name to set
   * @param value value to set
   * @return true if field is a new field in the hash and value was set, false if field already
   * exists and no operation was performed
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hSetNX(key: String)(field: String, value: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(HSetNX, key, field, value)(asInteger(toBoolean))

  /**
   * Returns all the values in a hash.
   *
   * @param key hash key
   * @return list of values, or the empty list if hash is empty or key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hVals[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): List[A] = send(HVals, key)(asMultiBulk[A, A, List](asBulk[A, A](flatten)))
  
  /**
   * Incrementally iterates through the fields of a hash.
   *
   * @param cursor the offset
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @return a pair containing the next cursor as its first element and the list of fields
   * (key-value pairs) as its second element
   *
   * @since 2.8.0
   */
  def hScan[A](key: String)(
    cursor: Long, countOpt: Option[Int] = None, matchOpt: Option[String] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): (Long, Set[(String, A)]) = send(
    generateScanLikeArgs(HScan, Some(key), cursor, countOpt, matchOpt): _*
  )(asScanMultiBulk[Set[(String, A)]](toPairsList[String, A](_).toSet))
  
}