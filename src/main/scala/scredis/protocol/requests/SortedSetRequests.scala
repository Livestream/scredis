package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object SortedSetRequests {
  
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
  private object SUnionStore extends Command("SUNIONSTORE")
  
  case class ZAdd[W: Writer](key: String, members: (scredis.Score, W)*) extends Request[Long](
    ZAdd,
    key,
    unpair(
      members.map {
        case (score, member) => (score.stringValue, implicitly[Writer[W]].write(member))
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
  
}