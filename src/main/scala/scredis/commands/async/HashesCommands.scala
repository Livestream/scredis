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

import scredis.CommandOptions
import scredis.parsing._

/**
 * This trait implements asynchronous hashes commands.
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait HashesCommands extends Async {

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
  ): Future[Long] = async(_.hDel(key)(field, fields: _*))

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
  ): Future[Boolean] = async(_.hExists(key)(field))

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
  ): Future[Option[A]] = async(_.hGet(key)(field))

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
  ): Future[Option[Map[String, A]]] = async(_.hGetAll(key))

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
  ): Future[Long] = async(_.hIncrBy(key)(field, count))

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
  ): Future[Double] = async(_.hIncrByFloat(key)(field, count))

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
  ): Future[Set[String]] = async(_.hKeys(key))

  /**
   * Returns the number of fields contained in the hash stored at key.
   *
   * @param key hash key
   * @return number of fields in the hash, or 0 if the key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hLen(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Long] =
    async(_.hLen(key))

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
  ): Future[List[Option[A]]] = async(_.hmGet(key)(field, fields: _*))

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
  ): Future[Map[String, A]] = async(_.hmGetAsMap(key)(field, fields: _*))

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
  ): Future[Unit] = async(_.hmSetFromMap(key, fieldValueMap))

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
  ): Future[Unit] = async(_.hmSet(key)(fieldValuePair, fieldValuePairs: _*))

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
  ): Future[Boolean] = async(_.hSet(key)(field, value))

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
  ): Future[Boolean] = async(_.hSetNX(key)(field, value))

  /**
   * Returns all the values in a hash.
   *
   * @param key hash key
   * @return list of values, or the empty if hash is empty or key does not exist
   * @throws $e if the value stored at key is not of type hash
   *
   * @since 2.0.0
   */
  def hVals[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Future[List[A]] = async(_.hVals(key))
  
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
  ): Future[(Long, Set[(String, A)])] = async(_.hScan(key)(cursor, countOpt, matchOpt))
  
}