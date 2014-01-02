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

import scredis.{ Score, Aggregate, CommandOptions }
import scredis.parsing._
import scredis.parsing.Implicits._
import scredis.protocol.Protocol
import scredis.exceptions.RedisCommandException
import scredis.Score._
import scredis.Aggregate._
import scredis.util.LinkedHashSet

/**
 * This trait implements sorted sets commands.
 *
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait SortedSetsCommands { self: Protocol =>
  protected val Aggregate = "AGGREGATE"
  protected val Weights = "WEIGHTS"
  protected val WithScores = "WITHSCORES"
  protected val Limit = "LIMIT"

  import Names._
  
  private implicit def stringToDouble(str: String): Double = augmentString(str).toDouble

  /**
   * Adds one or more members to a sorted set, or update its score if it already exists.
   *
   * @note If a specified member is already a member of the sorted set, the score is updated and
   * the element reinserted at the right position to ensure the correct ordering.
   *
   * @param key sorted set key
   * @param memberScoreMap member-score pairs to be added (adding several members at once only works
   * with Redis >= 2.4)
   * @return the number of elements added to the sorted sets, not including elements already
   * existing for which the score was updated
   * @throws $e if memberScoreMap is empty or key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zAddFromMap(key: String, memberScoreMap: Map[Any, Double])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = if(memberScoreMap.isEmpty) {
    throw RedisCommandException("ZADD: memberScoreMap cannot be empty")
  } else {
    send(flattenValueKeyMap(List(ZAdd, key), memberScoreMap): _*)(asInteger)
  }

  /**
   * Adds one or more members to a sorted set, or update its score if it already exists.
   *
   * @note If a specified member is already a member of the sorted set, the score is updated and
   * the element reinserted at the right position to ensure the correct ordering.
   *
   * @param key sorted set key
   * @param memberScorePair member-score pair to be added
   * @param memberScorePairs additional member-score pairs to be added (adding several members at
   * once only works with Redis >= 2.4)
   * @return the number of elements added to the sorted sets, not including elements already
   * existing for which the score was updated
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zAdd(key: String, memberScorePair: (Any, Double), memberScorePairs: (Any, Double)*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = zAddFromMap(key, (memberScorePair :: memberScorePairs.toList).map {
    case (member, score) => (member, score)
  }.toMap)

  /**
   * Returns the number of members in a sorted set.
   *
   * @param key sorted set key
   * @return the cardinality (number of elements) of the sorted set, or 0 if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zCard(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send(ZCard, key)(asInteger)

  /**
   * Returns the number of elements of a sorted set belonging to a given score range.
   *
   * @param key sorted set key
   * @param min score lower bound
   * @param max score upper bound
   * @return the number of elements in the specified score range
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zCount(key: String, min: Score, max: Score)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(ZCount, key, min.asMin, max.asMax)(asInteger)

  /**
   * Increments the score of a member in a sorted set.
   *
   * @param key sorted set key
   * @param member member whose score needs to be incremented
   * @param count the increment
   * @return the new score of member
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zIncrBy(key: String, member: Any, count: Double)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Double = send(ZIncrBy, key, count,member)(asBulk(toDouble))

  /**
   * Intersects multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param aggregate aggregation function (default is Sum)
   * @param key key of first sorted set
   * @param keys additional keys of sorted sets
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zInterStore(destKey: String, aggregate: Aggregate = Sum)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = {
    val count = keys.size + 1
    send((
      ZInterStore :: destKey :: count :: key :: keys.toList ::: Aggregate :: aggregate :: Nil
    ): _*)(asInteger)
  }

  /**
   * Intersects multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param aggregate aggregation function (default is Sum)
   * @param keyWeightPair first sorted set key to weight pair
   * @param keyWeightPairs additional sorted set key to weight pairs
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zInterStoreWeighted(destKey: String, aggregate: Aggregate = Sum)(
    keyWeightPair: (String, Int), keyWeightPairs: (String, Int)*
  )(implicit opts: CommandOptions = DefaultCommandOptions): Long = {
    val (keys, weights) = (keyWeightPair :: keyWeightPairs.toList).unzip
    send((
      ZInterStore ::
      destKey ::
      keys.size ::
      keys :::
      Weights ::
      weights :::
      Aggregate ::
      aggregate ::
      Nil
    ): _*)(asInteger)
  }

  /**
   * Computes the union of multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param aggregate aggregation function (default is Sum)
   * @param key key of first sorted set
   * @param keys additional keys of sorted sets
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zUnionStore(destKey: String, aggregate: Aggregate = Sum)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = {
    val count = keys.size + 1
    send((
      ZUnionStore :: destKey :: count :: key :: keys.toList ::: Aggregate :: aggregate :: Nil
    ): _*)(asInteger)
  }

  /**
   * Computes the union of multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param aggregate aggregation function (default is Sum)
   * @param keyWeightPair first sorted set key to weight pair
   * @param keyWeightPairs additional sorted set key to weight pairs
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zUnionStoreWeighted(destKey: String, aggregate: Aggregate = Sum)(
    keyWeightPair: (String, Int), keyWeightPairs: (String, Int)*
  )(implicit opts: CommandOptions = DefaultCommandOptions): Long = {
    val (keys, weights) = (keyWeightPair :: keyWeightPairs.toList).unzip
    send((
      ZUnionStore ::
      destKey ::
      keys.size ::
      keys :::
      Weights ::
      weights :::
      Aggregate ::
      aggregate ::
      Nil
    ): _*)(asInteger)
  }

  /**
   * Returns a range of members in a sorted set, by index.
   *
   * @note Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next
   * element and so on. They can also be negative numbers indicating offsets from the end of the
   * sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and
   * so on. Out of range indexes will not produce an error. If start is larger than the largest
   * index in the sorted set, or `start` > `end`, an empty list is returned. If `end` is larger
   * than the end of the sorted set Redis will treat it like it is the last element of the
   * sorted set. The indexes are inclusive.
   *
   * @param key sorted set key
   * @param start start offset (inclusive)
   * @param end end offset (inclusive)
   * @return the set of ascendingly ordered elements in the specified range, or the empty set if
   * key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRange[A](key: String, start: Long = 0, end: Long = -1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[A] = send(ZRange, key, start, end)(
    asMultiBulk[A, A, LinkedHashSet](asBulk[A, A](flatten))
  )

  /**
   * Returns a range of members with associated scores in a sorted set, by index.
   *
   * @note Both start and stop are zero-based indexes, where 0 is the first element, 1 is the next
   * element and so on. They can also be negative numbers indicating offsets from the end of the
   * sorted set, with -1 being the last element of the sorted set, -2 the penultimate element and
   * so on. Out of range indexes will not produce an error. If start is larger than the largest
   * index in the sorted set, or `start` > `end`, an empty list is returned. If `end` is larger
   * than the end of the sorted set Redis will treat it like it is the last element of the
   * sorted set. The indexes are inclusive.
   *
   * @param key sorted set key
   * @param start start offset (inclusive)
   * @param end end offset (inclusive)
   * @return the set of ascendingly ordered elements-score pairs in the specified range, or the
   * empty set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRangeWithScores[A](key: String, start: Long = 0, end: Long = -1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[(A, Double)] = send(ZRange, key, start, end, WithScores)(
    asMultiBulk[List, LinkedHashSet[(A, Double)]](toPairsLinkedHashSet[A, Double])
  )

  /**
   * Returns a range of members in a sorted set, by score.
   *
   * @note The elements having the same score are returned in lexicographical order (this follows
   * from a property of the sorted set implementation in Redis and does not involve further
   * computation).
   *
   * @param key sorted set key
   * @param min score lower bound
   * @param max score upper bound
   * @param limit optional offset and count pair used to limit the number of matching elements
   * @return the set of ascendingly ordered elements in the specified score range, or the empty set
   * if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.2.0
   */
  def zRangeByScore[A](
    key: String,
    min: Score,
    max: Score,
    limit: Option[(Long, Long)] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[A] = {
    val params = collection.mutable.MutableList[Any](ZRangeByScore, key, min.asMin, max.asMax)
    if (limit.isDefined) params ++= Limit :: limit.get._1 :: limit.get._2 :: Nil
    send(params: _*)(asMultiBulk[A, A, LinkedHashSet](asBulk[A, A](flatten)))
  }

  /**
   * Returns a range of members with associated scores in a sorted set, by score.
   *
   * @note The elements having the same score are returned in lexicographical order (this follows
   * from a property of the sorted set implementation in Redis and does not involve further
   * computation).
   *
   * @param key sorted set key
   * @param min score lower bound
   * @param max score upper bound
   * @param limit optional offset and count pair used to limit the number of matching elements
   * @return the set of ascendingly ordered element-score pairs in the specified score range, or
   * the empty set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRangeByScoreWithScores[A](
    key: String,
    min: Score,
    max: Score,
    limit: Option[(Long, Long)] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[(A, Double)] = {
    val params = collection.mutable.MutableList[Any](
      ZRangeByScore, key, min.asMin, max.asMax, WithScores
    )
    if (limit.isDefined) params ++= Limit :: limit.get._1 :: limit.get._2 :: Nil
    send(params: _*)(
      asMultiBulk[List, LinkedHashSet[(A, Double)]](toPairsLinkedHashSet[A, Double])
    )
  }

  /**
   * Determines the index of a member in a sorted set.
   *
   * @param key sorted set key
   * @param member the value
   * @return the rank or index of the member, or $none if the member is not in the set or the key
   * does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRank(key: String, member: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Option[Long] = send(ZRank, key,member)(asIntegerOrNullBulkReply)

  /**
   * Removes one or more members from a sorted set.
   *
   * @note Redis versions older than 2.4 can only remove one value per call.
   *
   * @param key sorted set key
   * @param member the value to be removed
   * @param members additional values to be removed (only works with Redis >= 2.4)
   * @return the number of members removed from the sorted set, not including non existing members
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRem(key: String, member: Any, members: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(ZRem :: key ::member ::members.toList: _*)(asInteger)

  /**
   * Removes all members in a sorted set within the given indexes.
   *
   * @note Both start and stop are zero-based inclusive indexes with 0 being the element with the
   * lowest score. These indexes can be negative numbers, where they indicate offsets starting at
   * the element with the highest score. For example: -1 is the element with the highest score, -2
   * the element with the second highest score and so forth.
   *
   * @param key sorted set key
   * @param start the start offset or index (inclusive)
   * @param end the end offset or index (inclusive)
   * @return the number of members removed
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRemRangeByRank(key: String, start: Long, end: Long)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(ZRemRangeByRank, key, start, end)(asInteger)

  /**
   * Removes all members in a sorted set within the given scores range.
   *
   * @note Since version 2.1.6, min and max can be exclusive, following the syntax of ZRANGEBYSCORE.
   *
   * @param key sorted set key
   * @param min score lower bound
   * @param max score upper bound
   * @return the number of members removed
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRemRangeByScore(key: String, min: Score, max: Score)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(ZRemRangeByScore, key, min.asMin, max.asMax)(asInteger)

  /**
   * Returns a range of members in a sorted set, by index, with scores ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
   *
   * @param key sorted set key
   * @param start start offset (inclusive)
   * @param end end offset (inclusive)
   * @return the set of descendingly ordered elements in the specified range, or the empty set if
   * key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRevRange[A](key: String, start: Long = 0, end: Long = -1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[A] = send(ZRevRange, key, start, end)(
    asMultiBulk[A, A, LinkedHashSet](asBulk[A, A](flatten))
  )

  /**
   * Returns a range of members in a sorted set, by index, with scores ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
   *
   * @param key sorted set key
   * @param start start offset (inclusive)
   * @param end end offset (inclusive)
   * @return the set of descendingly ordered elements-score pairs in the specified range, or the
   * empty set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRevRangeWithScores[A](key: String, start: Long = 0, end: Long = -1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[(A, Double)] = send(ZRevRange, key, start, end, WithScores)(
    asMultiBulk[List, LinkedHashSet[(A, Double)]](toPairsLinkedHashSet[A, Double])
  )

  /**
   * Returns a range of members in a sorted set, by score, with scores ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE.
   *
   * @param key sorted set key
   * @param max score upper bound
   * @param min score lower bound
   * @param limit optional offset and count pair used to limit the number of matching elements
   * @return the set of descendingly ordered elements in the specified score range, or the empty
   * set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.2.0
   */
  def zRevRangeByScore[A](
    key: String,
    max: Score,
    min: Score,
    limit: Option[(Long, Long)] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[A] = {
    val params = collection.mutable.MutableList[Any](ZRevRangeByScore, key, max.asMax, min.asMin)
    if (limit.isDefined) params ++= Limit :: limit.get._1 :: limit.get._2 :: Nil
    send(params: _*)(asMultiBulk[A, A, LinkedHashSet](asBulk[A, A](flatten)))
  }

  /**
   * Return a range of members with associated scores in a sorted set, by score, with scores
   * ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE.
   *
   * @param key sorted set key
   * @param max score upper bound
   * @param min score lower bound
   * @param limit optional offset and count pair used to limit the number of matching elements
   * @return the set of descendingly ordered elements in the specified score range, or the empty
   * set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRevRangeByScoreWithScores[A](
    key: String,
    max: Score,
    min: Score,
    limit: Option[(Long, Long)] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): LinkedHashSet[(A, Double)] = {
    val params = collection.mutable.MutableList[Any](
      ZRevRangeByScore, key, max.asMax, min.asMin, WithScores
    )
    if (limit.isDefined) params ++= Limit :: limit.get._1 :: limit.get._2 :: Nil
    send(params: _*)(
      asMultiBulk[List, LinkedHashSet[(A, Double)]](toPairsLinkedHashSet[A, Double])
    )
  }

  /**
   * Determine the index of a member in a sorted set, with scores ordered from high to low.
   *
   * @param key sorted set key
   * @param member the value
   * @return the rank or index of the member, or $none if the member is not in the set or the key
   * does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRevRank(key: String, member: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Option[Long] = send(ZRevRank, key,member)(asIntegerOrNullBulkReply)

  /**
   * Returns the score associated with the given member in a sorted set.
   *
   * @param key sorted set key
   * @param member the value
   * @return the score of member, or $none if the latter is not in the sorted set or the key does
   * not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zScore(key: String, member: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Option[Double] = send(ZScore, key,member)(asBulk[Double])
  
  /**
   * Incrementally iterates the elements (value-score pairs) of a sorted set.
   *
   * @param cursor the offset
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @return a pair containing the next cursor as its first element and the sorted set of
   * elements (value-score pairs) as its second element
   *
   * @since 2.8.0
   */
  def zScan[A](key: String)(
    cursor: Long, countOpt: Option[Int] = None, matchOpt: Option[String] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): (Long, LinkedHashSet[(A, Double)]) = send(
    generateScanLikeArgs(ZScan, Some(key), cursor, countOpt, matchOpt): _*
  )(asScanMultiBulk[LinkedHashSet[(A, Double)]](toPairsLinkedHashSet))
  
}