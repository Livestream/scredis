package scredis.commands

import scredis.AbstractClient
import scredis.protocol.requests.SortedSetRequests._
import scredis.serialization.{ Reader, Writer }
import scredis.util.LinkedHashSet

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements sorted set commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait SortedSetCommands { self: AbstractClient =>
  
  /**
   * Adds a member to a sorted set, or update its score if it already exists.
   *
   * @note If a specified member is already a member of the sorted set, the score is updated and
   * the element reinserted at the right position to ensure the correct ordering.
   *
   * @param key sorted set key
   * @param member member to add
   * @param score score of the member to add
   * @return $true if the member was added, or $false if the member already exists
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zAdd[W: Writer](key: String, member: W, score: scredis.Score): Future[Boolean] = send(
    ZAdd(key, Map(member -> score))
  ).map {
    case 0 => false
    case x => true
  }

  /**
   * Adds one or more members to a sorted set, or update its score if it already exists.
   *
   * @note If a specified member is already a member of the sorted set, the score is updated and
   * the element reinserted at the right position to ensure the correct ordering.
   *
   * @param key sorted set key
   * @param members member-score pairs to be added
   * @return the number of elements added to the sorted sets, not including elements already
   * existing for which the score was updated
   * @throws $e if key contains a value that is not a sorted set or if members is empty
   *
   * @since 2.4
   */
  def zAdd[W: Writer](key: String, members: Map[W, scredis.Score]): Future[Long] = send(
    ZAdd(key, members)
  )
  
  /**
   * Returns the number of members in a sorted set.
   *
   * @param key sorted set key
   * @return the cardinality (number of elements) of the sorted set, or 0 if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zCard(key: String): Future[Long] = send(ZCard(key))
  
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
  def zCount(key: String, min: scredis.ScoreLimit, max: scredis.ScoreLimit): Future[Long] = send(
    ZCount(key, min, max)
  )
  
  /**
   * Increments the score of a member in a sorted set.
   *
   * @param key sorted set key
   * @param member member whose score needs to be incremented
   * @param increment the increment
   * @return the new score of member
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zIncrBy[W: Writer](key: String, member: W, increment: Double): Future[Double] = send(
    ZIncrBy(key, increment, member)
  )
  
  /**
   * Intersects multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param keys keys of sorted sets to intersect
   * @param aggregate aggregation function (default is Sum)
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zInterStore(
    destKey: String,
    keys: Seq[String],
    aggregate: scredis.Aggregate = scredis.Aggregate.Sum
  ): Future[Long] = send(ZInterStore(destKey, keys, aggregate))
  
  /**
   * Intersects multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param keysWeightPairs key to weight pairs
   * @param aggregate aggregation function (default is Sum)
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zInterStoreWeighted(
    destKey: String,
    keysWeightPairs: Map[String, Double],
    aggregate: scredis.Aggregate = scredis.Aggregate.Sum
  ): Future[Long] = send(ZInterStoreWeighted(destKey, keysWeightPairs, aggregate))
  
  /**
   * Returns the number of elements of a sorted set belonging to a given lexical score range.
   * 
   * @note Lexical ordering only applies when all the elements in a sorted set are inserted
   * with the same score
   * 
   * @param key sorted set key
   * @param min lexical score lower bound
   * @param max lexical score upper bound
   * @return the number of elements in the specified lexical score range
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zLexCount(
    key: String, min: scredis.LexicalScoreLimit, max: scredis.LexicalScoreLimit
  ): Future[Long] = send(ZLexCount(key, min, max))
  
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
   * @param stop stop offset (inclusive)
   * @return the set of ascendingly ordered elements in the specified range, or the empty set if
   * key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRange[R: Reader](
    key: String, start: Long = 0, stop: Long = -1
  ): Future[LinkedHashSet[R]] = send(ZRange[R, LinkedHashSet](key, start, stop))
  
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
   * @param stop end offset (inclusive)
   * @return the set of ascendingly ordered elements-score pairs in the specified range, or the
   * empty set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRangeWithScores[R: Reader](
    key: String, start: Long = 0, stop: Long = -1
  ): Future[LinkedHashSet[(R, scredis.Score)]] = send(
    ZRangeWithScores[R, LinkedHashSet](key, start, stop)
  )
  
  /**
   * Returns a range of members in a sorted set, by index.
   *
   * @param key sorted set key
   * @param start start offset (inclusive)
   * @param stop stop offset (inclusive)
   * @return the set of ascendingly ordered elements in the specified range, or the empty set if
   * key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRangeByLex[R: Reader](
    key: String, start: Long = 0, stop: Long = -1
  ): Future[LinkedHashSet[R]] = send(ZRangeByLex[R, LinkedHashSet](key, start, stop))
  
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
  def zRangeByScore[R: Reader](
    key: String,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)] = None
  ): Future[LinkedHashSet[R]] = send(
    ZRangeByScore[R, LinkedHashSet](key, min, max, limitOpt)
  )
  
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
  def zRangeByScoreWithScores[R: Reader](
    key: String,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)] = None
  ): Future[LinkedHashSet[(R, scredis.Score)]] = send(
    ZRangeByScoreWithScores[R, LinkedHashSet](key, min, max, limitOpt)
  )
  
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
   * @param stop the stop offset or index (inclusive)
   * @return the number of members removed
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zRemRangeByRank(key: String, start: Long, stop: Long)(
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
   * @param stop stop offset (inclusive)
   * @return the set of descendingly ordered elements in the specified range, or the empty set if
   * key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRevRange[A](key: String, start: Long = 0, stop: Long = -1)(
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
   * @param stop stop offset (inclusive)
   * @return the set of descendingly ordered elements-score pairs in the specified range, or the
   * empty set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 1.2.0
   */
  def zRevRangeWithScores[A](key: String, start: Long = 0, stop: Long = -1)(
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
  
  /**
   * Computes the union of multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param keys keys of sorted sets
   * @param aggregate aggregation function (default is Sum)
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zUnionStore(
    destKey: String,
    keys: Seq[String],
    aggregate: scredis.Aggregate = scredis.Aggregate.Sum
  ): Future[Long] = send(ZUnionStore(destKey, keys, aggregate))
  
  /**
   * Computes the union of multiple sorted sets and stores the resulting sorted set in a new key.
   *
   * @param destKey sorted set key
   * @param keyWeightPairs key to weight pairs
   * @param aggregate aggregation function (default is Sum)
   * @return the number of elements in the resulting sorted set stored at destKey
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.0.0
   */
  def zUnionStoreWeighted(
    destKey: String,
    keysWeightPairs: Map[String, Double],
    aggregate: scredis.Aggregate = scredis.Aggregate.Sum
  ): Future[Long] = send(ZInterStoreWeighted(destKey, keysWeightPairs, aggregate))
  
}
