package scredis.commands

import scredis.io.Connection
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
trait SortedSetCommands { self: Connection =>
  
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
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.4
   */
  def zAdd[W: Writer](key: String, members: Map[W, scredis.Score]): Future[Long] = {
    if (members.isEmpty) {
      Future.successful(0)
    } else {
      send(ZAdd(key, members))
    }
  }
  
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
   * @since 2.8.9
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
   * Returns a range of members in a sorted set, by lexical score.
   *
   * @note Lexical ordering only applies when all the elements in a sorted set are inserted
   * with the same score
   * 
   * @param key sorted set key
   * @param min lexical score lower bound
   * @param max lexical score upper bound
   * @param limitOpt optional offset and count pair used to limit the number of matching elements
   * @return the set of ascendingly ordered elements in the specified lexical range, or the empty
   * set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.8.9
   */
  def zRangeByLex[R: Reader](
    key: String,
    min: scredis.LexicalScoreLimit,
    max: scredis.LexicalScoreLimit,
    limitOpt: Option[(Long, Int)] = None
  ): Future[LinkedHashSet[R]] = send(ZRangeByLex[R, LinkedHashSet](key, min, max, limitOpt))
  
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
   * @param limitOpt optional offset and count pair used to limit the number of matching elements
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
   * @param limitOpt optional offset and count pair used to limit the number of matching elements
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
  def zRank[W: Writer](key: String, member: W): Future[Option[Long]] = send(
    ZRank(key, member)
  )
  
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
  def zRem[W: Writer](key: String, members: W*): Future[Long] = send(
    ZRem(key, members: _*)
  )
  
  /**
   * Removes all members in a sorted set within the given lexical range.
   *
   * @note Lexical ordering only applies when all the elements in a sorted set are inserted
   * with the same score
   * 
   * @param key sorted set key
   * @param min lexical score lower bound
   * @param max lexical score upper bound
   * @return the number of removed elements
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.8.9
   */
  def zRemRangeByLex(
    key: String,
    min: scredis.LexicalScoreLimit,
    max: scredis.LexicalScoreLimit
  ): Future[Long] = send(ZRemRangeByLex(key, min, max))
  
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
  def zRemRangeByRank(key: String, start: Long, stop: Long): Future[Long] = send(
    ZRemRangeByRank(key, start, stop)
  )
  
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
  def zRemRangeByScore(
    key: String, min: scredis.ScoreLimit, max: scredis.ScoreLimit
  ): Future[Long] = send(
    ZRemRangeByScore(key, min, max)
  )
  
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
  def zRevRange[R: Reader](
    key: String, start: Long = 0, stop: Long = -1
  ): Future[LinkedHashSet[R]] = send(
    ZRevRange[R, LinkedHashSet](key, start, stop)
  )
  
  /**
   * Returns a range of members in a sorted set, by index, with scores ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE. The elements having
   * the same score are returned in reverse lexicographical order.
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
  def zRevRangeWithScores[R: Reader](
    key: String, start: Long = 0, stop: Long = -1
  ): Future[LinkedHashSet[(R, scredis.Score)]] = send(
    ZRevRangeWithScores[R, LinkedHashSet](key, start, stop)
  )
  
  /**
   * Returns a range of members in a sorted set, by score, with scores ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE. The
   * elements having the same score are returned in reverse lexicographical order.
   *
   * @param key sorted set key
   * @param max score upper bound
   * @param min score lower bound
   * @param limitOpt optional offset and count pair used to limit the number of matching elements
   * @return the set of descendingly ordered elements in the specified score range, or the empty
   * set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.2.0
   */
  def zRevRangeByScore[R: Reader](
    key: String,
    max: scredis.ScoreLimit,
    min: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)] = None
  ): Future[LinkedHashSet[R]] = send(
    ZRevRangeByScore[R, LinkedHashSet](key, max, min, limitOpt)
  )
  
  /**
   * Return a range of members with associated scores in a sorted set, by score, with scores
   * ordered from high to low.
   *
   * @note Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to ZRANGEBYSCORE. The
   * elements having the same score are returned in reverse lexicographical order.
   *
   * @param key sorted set key
   * @param max score upper bound
   * @param min score lower bound
   * @param limitOpt optional offset and count pair used to limit the number of matching elements
   * @return the set of descendingly ordered elements in the specified score range, or the empty
   * set if key does not exist
   * @throws $e if key contains a value that is not a sorted set
   *
   * @since 2.2.0
   */
  def zRevRangeByScoreWithScores[R: Reader](
    key: String,
    max: scredis.ScoreLimit,
    min: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)] = None
  ): Future[LinkedHashSet[(R, scredis.Score)]] = send(
    ZRevRangeByScoreWithScores[R, LinkedHashSet](key, max, min, limitOpt)
  )
  
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
  def zRevRank[W: Writer](key: String, member: W): Future[Option[Long]] = send(
    ZRevRank(key, member)
  )
  
  /**
   * Incrementally iterates the elements (value-score pairs) of a sorted set.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the sorted set of
   * elements (value-score pairs) as its second element
   *
   * @since 2.8.0
   */
  def zScan[R: Reader](
    key: String,
    cursor: Long,
    matchOpt: Option[String] = None,
    countOpt: Option[Int] = None
  ): Future[(Long, LinkedHashSet[(R, scredis.Score)])] = send(
    ZScan[R, LinkedHashSet](
      key = key,
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    )
  )
  
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
  def zScore[W: Writer](key: String, member: W): Future[Option[scredis.Score]] = send(
    ZScore(key, member)
  )
  
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
  ): Future[Long] = send(ZUnionStoreWeighted(destKey, keysWeightPairs, aggregate))
  
}
