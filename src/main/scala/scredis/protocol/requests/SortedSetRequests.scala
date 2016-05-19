package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{Reader, Writer}

import scala.collection.generic.CanBuildFrom

object SortedSetRequests {
  
  import scredis.serialization.Implicits.{doubleReader, stringReader}
  
  object ZAdd extends Command("ZADD") with WriteCommand
  object ZCard extends Command("ZCARD")
  object ZCount extends Command("ZCOUNT")
  object ZIncrBy extends Command("ZINCRBY") with WriteCommand
  object ZInterStore extends Command("ZINTERSTORE") with WriteCommand
  object ZLexCount extends Command("ZLEXCOUNT")
  object ZRange extends Command("ZRANGE")
  object ZRangeByLex extends Command("ZRANGEBYLEX")
  object ZRangeByScore extends Command("ZRANGEBYSCORE")
  object ZRank extends Command("ZRANK")
  object ZRem extends Command("ZREM") with WriteCommand
  object ZRemRangeByLex extends Command("ZREMRANGEBYLEX") with WriteCommand
  object ZRemRangeByRank extends Command("ZREMRANGEBYRANK") with WriteCommand
  object ZRemRangeByScore extends Command("ZREMRANGEBYSCORE") with WriteCommand
  object ZRevRange extends Command("ZREVRANGE")
  object ZRevRangeByScore extends Command("ZREVRANGEBYSCORE")
  object ZRevRank extends Command("ZREVRANK")
  object ZScan extends Command("ZSCAN")
  object ZScore extends Command("ZSCORE")
  object ZUnionStore extends Command("ZUNIONSTORE") with WriteCommand
  
  private val WithScores = "WITHSCORES"
  private val Weights = "WEIGHTS"
  private val AggregateName = "AGGREGATE"
  private val Limit = "LIMIT"
  
  case class ZAdd[K, W](key: K, members: Map[W, scredis.Score])(
    implicit keyWriter: Writer[K], writer: Writer[W]
  ) extends Request[Long](
    ZAdd, keyWriter.write(key) +: unpair(
      (members toList).map {
        case (member, score) => (score.stringValue, writer.write(member))
      }
    ): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZCard[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    ZCard, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZCount[K](
    key: K, min: scredis.ScoreLimit, max: scredis.ScoreLimit
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    ZCount, keyWriter.write(key), min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZIncrBy[K, W: Writer](
    key: K, increment: Double, member: W
  )(implicit keyWriter: Writer[K]) extends Request[Double](
    ZIncrBy, keyWriter.write(key), increment, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class ZInterStore[KD, K](
    destination: KD, keys: Seq[K], aggregate: scredis.Aggregate
  )(implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]) extends Request[Long](
    ZInterStore,
    destKeyWriter.write(destination) +: keys.size +: keys.map(keyWriter.write) :+ AggregateName :+ aggregate.name: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZInterStoreWeighted[KD, K](
    destination: KD, keyWeightPairs: Map[K, Double], aggregate: scredis.Aggregate
  )(implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]) extends Request[Long](
    ZInterStore, destKeyWriter.write(destination) :: keyWeightPairs.size :: {
      val (keys, weights) = keyWeightPairs.toList.unzip
      keys.map(keyWriter.write) ::: Weights :: weights ::: AggregateName :: aggregate.name :: Nil
    }: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZLexCount[K](
    key: K, min: scredis.LexicalScoreLimit, max: scredis.LexicalScoreLimit
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    ZLexCount, keyWriter.write(key), min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRange[K, R: Reader, CC[X] <: Traversable[X]](key: K, start: Long, stop: Long)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    ZRange, keyWriter.write(key), start, stop
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRangeWithScores[K, R: Reader, CC[X] <: Traversable[X]](
    key: K, start: Long, stop: Long
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](
    ZRange, keyWriter.write(key), start, stop, WithScores
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
        case b: BulkStringResponse => b.flattened[R]
      } {
        case b: BulkStringResponse => scredis.Score(b.flattened[String])
      }
    }
  }
  
  case class ZRangeByLex[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    min: scredis.LexicalScoreLimit,
    max: scredis.LexicalScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRangeByLex, keyWriter.write(key) +: min.stringValue +: max.stringValue +: {
      limitOpt match {
        case Some((offset, count)) => Seq(Limit, offset, count)
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
  
  case class ZRangeByScore[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRangeByScore, keyWriter.write(key) +: min.stringValue +: max.stringValue +: {
      limitOpt match {
        case Some((offset, count)) => Seq(Limit, offset, count)
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
  
  case class ZRangeByScoreWithScores[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    min: scredis.ScoreLimit,
    max: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](
    ZRangeByScore, keyWriter.write(key) +: min.stringValue +: max.stringValue +: WithScores +: {
      limitOpt match {
        case Some((offset, count)) => Seq(Limit, offset, count)
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
  
  case class ZRank[K, W: Writer](key: K, member: W)(implicit keyWriter: Writer[K]) extends Request[Option[Long]](
    ZRank, keyWriter.write(key), implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class ZRem[K, W](key: K, members: W*)(implicit keyWriter: Writer[K], writer: Writer[W]) extends Request[Long](
    ZRem, keyWriter.write(key) +: members.map(writer.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByLex[K](
    key: K, min: scredis.LexicalScoreLimit, max: scredis.LexicalScoreLimit
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    ZRemRangeByLex, keyWriter.write(key), min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByRank[K](key: K, start: Long, stop: Long)(implicit keyWriter: Writer[K]) extends Request[Long](
    ZRemRangeByRank, keyWriter.write(key), start, stop
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRemRangeByScore[K](
    key: K, min: scredis.ScoreLimit, max: scredis.ScoreLimit
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    ZRemRangeByScore, keyWriter.write(key), min.stringValue, max.stringValue
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZRevRange[K, R: Reader, CC[X] <: Traversable[X]](key: K, start: Long, stop: Long)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    ZRevRange, keyWriter.write(key), start, stop
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class ZRevRangeWithScores[K, R: Reader, CC[X] <: Traversable[X]](
    key: K, start: Long, stop: Long
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](
    ZRevRange, keyWriter.write(key), start, stop, WithScores
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[R, scredis.Score, CC] {
        case b: BulkStringResponse => b.flattened[R]
      } {
        case b: BulkStringResponse => scredis.Score(b.flattened[String])
      }
    }
  }
  
  case class ZRevRangeByScore[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    max: scredis.ScoreLimit,
    min: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[CC[R]](
    ZRevRangeByScore, keyWriter.write(key) +: max.stringValue +: min.stringValue +: {
      limitOpt match {
        case Some((offset, count)) => Seq(Limit, offset, count)
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
  
  case class ZRevRangeByScoreWithScores[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    max: scredis.ScoreLimit,
    min: scredis.ScoreLimit,
    limitOpt: Option[(Long, Int)]
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
  ) extends Request[CC[(R, scredis.Score)]](
    ZRevRangeByScore, keyWriter.write(key) +: max.stringValue +: min.stringValue +: WithScores +: {
      limitOpt match {
        case Some((offset, count)) => Seq(Limit, offset, count)
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
  
  case class ZRevRank[K, W: Writer](key: K, member: W)(implicit keyWriter: Writer[K]) extends Request[Option[Long]](
    ZRevRank, keyWriter.write(key), implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class ZScan[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (R, scredis.Score), CC[(R, scredis.Score)]]
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
  
  case class ZScore[K, W: Writer](key: K, member: W)(implicit keyWriter: Writer[K]) extends Request[Option[scredis.Score]](
    ZScore, keyWriter.write(key), implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String].map(scredis.Score.apply)
    }
  }
  
  case class ZUnionStore[KD, K](
    destination: KD, keys: Seq[K], aggregate: scredis.Aggregate
  )(
    implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Long](
    ZUnionStore,
    destKeyWriter.write(destination) +: keys.size +: keys.map(keyWriter.write) :+ AggregateName :+ aggregate.name: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ZUnionStoreWeighted[KD, K](
    destination: KD, keyWeightPairs: Map[K, Double], aggregate: scredis.Aggregate
  )(
    implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Long](
    ZUnionStore, destKeyWriter.write(destination) :: keyWeightPairs.size :: {
      val (keys, weights) = keyWeightPairs.toList.unzip
      keys.map(keyWriter.write) ::: Weights :: weights ::: AggregateName :: aggregate.name :: Nil
    }: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
}