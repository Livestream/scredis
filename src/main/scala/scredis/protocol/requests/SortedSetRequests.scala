package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object SortedSetRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.doubleReader
  
  private object ZAdd extends Command("ZADD")
  private object ZCard extends Command("ZCARD")
  private object ZCount extends Command("ZCOUNT")
  private object ZIncrBy extends Command("ZINCRBY")
  private object ZInterStore extends Command("ZINTERSTORE")
  private object ZLexCount extends Command("ZLEXCOUNT")
  private object ZRange extends Command("ZRANGE")
  private object ZRangeByLex extends Command("ZRANGEBYLEX")
  private object ZRangeByScore extends Command("ZRANGEBYSCORE")
  private object ZRank extends Command("ZRANK")
  private object ZRem extends Command("ZREM")
  private object ZRemRangeByLex extends Command("ZREMRANGEBYLEX")
  private object ZRemRangeByRank extends Command("ZREMRANGEBYRANK")
  private object ZRemRangeByScore extends Command("ZREMRANGEBYSCORE")
  private object ZRevRange extends Command("ZREVRANGE")
  private object ZRevRangeByScore extends Command("ZREVRANGEBYSCORE")
  private object ZRevRank extends Command("ZREVRANK")
  private object ZScan extends Command("ZSCAN")
  private object ZScore extends Command("ZSCORE")
  private object ZUnionStore extends Command("ZUNIONSTORE")
  
  private val WithScores = "WITHSCORES"
  private val Weights = "WEIGHTS"
  private val Aggregate = "AGGREGATE"
  
  case class ZAdd[W: Writer](key: String, members: (W, scredis.Score)*) extends Request[Long](
    ZAdd,
    key,
    unpair(
      members.map {
        case (member, score) => (score.stringValue, implicitly[Writer[W]].write(member))
      }
    ): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZCard(key: String) extends Request[Long](ZCard, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZCount(
    key: String, min: scredis.ScoreLimit, max: scredis.ScoreLimit
  ) extends Request[Long](ZCount, key, min.stringValue, max.stringValue) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZIncrBy[W: Writer](key: String, increment: Double, member: W) extends Request[Double](
    ZIncrBy, key, increment, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class ZInterStore(
    destination: String, aggregate: scredis.Aggregate, keys: String*
  ) extends Request[Long](
    ZInterStore, destination, keys.size, keys: _*, Aggregate, aggregate.name
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZInterStoreWeighted(
    destination: String, aggregate: scredis.Aggregate, keyWeightPairs: (String, Double)*
  ) extends Request[Long](
    ZInterStore,
    destination,
    keyWeightPairs.size,
    {
      val (keys, weights) = keyWeightPairs.toList.unzip
      keys ::: Weights :: weights ::: Aggregate :: aggregate.name :: Nil
    }: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZLexCount(
    key: String, min: scredis.LexicalScoreLimit, max: scredis.LexicalScoreLimit
  ) extends Request[Long](ZLexCount, key, min.stringValue, max.stringValue) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRange[R: Reader, CC[X] <: Traversable[X]](key: String, start: Long, stop: Long)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](ZRange, key, start, stop) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRangeWithScores[R: Reader, CC[X] <: Traversable[X]](
    key: String, start: Long, stop: Long
  )(
    implicit cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](ZRange, key, start, stop, WithScores) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
        case b: BulkStringResponse => b.flattened[R]
      } {
        case b: BulkStringResponse => scredis.Score(b.flattened[String])
      }
    }
  }
  
  case class ZRangeByLex[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    min: scredis.LexicalScoreLimit,
    max: scredis.LexicalScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRangeByLex,
    key,
    min.stringValue,
    max.stringValue,
    {
      limitOpt match {
        case Some((offset, count)) => Seq(offset, count)
        case None => Seq.empty
      }
    }: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRangeByScore[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRangeByScore,
    key,
    min.stringValue,
    max.stringValue,
    {
      limitOpt match {
        case Some((offset, count)) => Seq(offset, count)
        case None => Seq.empty
      }
    }: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRangeByScoreWithScores[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(
    implicit cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](
    ZRangeByScore,
    key,
    min.stringValue,
    max.stringValue,
    WithScores,
    {
      limitOpt match {
        case Some((offset, count)) => Seq(offset, count)
        case None => Seq.empty
      }
    }: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
        case b: BulkStringResponse => b.flattened[R]
      } {
        case b: BulkStringResponse => scredis.Score(b.flattened[String])
      }
    }
  }
  
  case class ZRank[W: Writer](key: String, member: W) extends Request[Option[Long]](
    ZRank, key, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class ZRem[W: Writer](key: String, members: W*) extends Request[Long](
    ZRem, key, members.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByLex(
    key: String, min: scredis.LexicalScoreLimit, max: scredis.LexicalScoreLimit
  ) extends Request[Long](
    ZRemRangeByLex, key, min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByRank(key: String, start: Long, stop: Long) extends Request[Long](
    ZRemRangeByRank, key, start, stop
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByScore(
    key: String, min: scredis.ScoreLimit, max: scredis.ScoreLimit
  ) extends Request[Long](
    ZRemRangeByScore, key, min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRevRange[R: Reader, CC[X] <: Traversable[X]](key: String, start: Long, stop: Long)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](ZRevRange, key, start, stop) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRevRangeWithScores[R: Reader, CC[X] <: Traversable[X]](
    key: String, start: Long, stop: Long
  )(
    implicit cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](ZRevRange, key, start, stop, WithScores) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
        case b: BulkStringResponse => b.flattened[R]
      } {
        case b: BulkStringResponse => scredis.Score(b.flattened[String])
      }
    }
  }
  
  case class ZRevRangeByScore[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    max: scredis.ScoreLimit,
    min: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRevRangeByScore,
    key,
    max.stringValue,
    min.stringValue,
    {
      limitOpt match {
        case Some((offset, count)) => Seq(offset, count)
        case None => Seq.empty
      }
    }: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRevRank[W: Writer](key: String, member: W) extends Request[Option[Long]](
    ZRevRank, key, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class ZScan[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(
    implicit cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[(Long, CC[(R, scredis.Score)])](
    ZScan,
    generateScanLikeArgs(
      keyOpt = Some(key),
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    ): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[(R, scredis.Score), CC] {
        case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
          case b: BulkStringResponse => b.flattened[R]
        } {
          case b: BulkStringResponse => scredis.Score(b.flattened[String])
        }
      }
    }
  }
  
  case class ZScore[W: Writer](key: String, member: W) extends Request[Option[scredis.Score]](
    ZScore, key, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String].map(scredis.Score.apply)
    }
  }
  
  case class ZUnionStore(
    destination: String, aggregate: scredis.Aggregate, keys: String*
  ) extends Request[Long](
    ZUnionStore, destination, keys.size, keys: _*, Aggregate, aggregate.name
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZUnionStoreWeighted(
    destination: String, aggregate: scredis.Aggregate, keyWeightPairs: (String, Double)*
  ) extends Request[Long](
    ZUnionStore,
    destination,
    keyWeightPairs.size,
    {
      val (keys, weights) = keyWeightPairs.toList.unzip
      keys ::: Weights :: weights ::: Aggregate :: aggregate.name :: Nil
    }: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
}