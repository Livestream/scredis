package scredis.protocol.requests

import scredis.protocol._
import scredis.parsing.Parser

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

object HashRequests {
  
  import scredis.parsing.Implicits.stringParser
  import scredis.parsing.Implicits.doubleParser
  
  private object HDel extends Command("HDEL")
  private object HExists extends Command("HEXISTS")
  private object HGet extends Command("HGET")
  private object HGetAll extends Command("HGETALL")
  private object HIncrBy extends Command("HINCRBY")
  private object HIncrByFloat extends Command("HINCRBYFLOAT")
  private object HKeys extends Command("HKEYS")
  private object HLen extends Command("HLEN")
  private object HMGet extends Command("HMGET")
  private object HMSet extends Command("HMSET")
  private object HScan extends Command("HSCAN")
  private object HSet extends Command("HSET")
  private object HSetNX extends Command("HSETNX")
  private object HVals extends Command("HVALS")
  
  case class HDel(keys: String*) extends Request[Long](HDel, keys: _*) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HExists(key: String) extends Request[Boolean](HExists, key) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HGet[A](key: String, field: String)(
    implicit parser: Parser[A]
  ) extends Request[Option[A]](HGet, key, field) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[A]
    }
  }
  
  case class HGetAll[A](key: String)(
    implicit parser: Parser[A]
  ) extends Request[Map[String, A]](HGetAll, key) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, A, Map] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[A]
      }
    }
  }
  
  case class HIncrBy(key: String, field: String, value: Long) extends Request[Long](
    HIncrBy, key, field, value
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HIncrByFloat(key: String, field: String, value: Double) extends Request[Double](
    HIncrByFloat, key, field, value
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class HKeys(key: String) extends Request[Set[String]](HKeys, key) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, Set] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class HLen(key: String) extends Request[Long](HLen, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HMGet[A, CC[X] <: Traversable[X]](key: String, fields: String*)(
    implicit parser: Parser[A], cbf: CanBuildFrom[Nothing, Option[A], CC[Option[A]]]
  ) extends Request[CC[Option[A]]](HMGet, key, fields: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[A], CC] {
        case b: BulkStringResponse => b.parsed[A]
      }
    }
  }
  
  case class HMGetAsMap[A, CC[X] <: Traversable[X]](key: String, fields: String*)(
    implicit parser: Parser[A], cbf: CanBuildFrom[Nothing, Option[A], CC[Option[A]]]
  ) extends Request[Map[String, A]](HMGet, key, fields: _*) {
    override def decode = {
      case a: ArrayResponse => {
        val values = a.parsed[Option[A], List] {
          case b: BulkStringResponse => b.parsed[A]
        }
        fields.zip(values).flatMap {
          case (key, Some(value)) => Some((key, value))
          case _ => None
        }.toMap
      }
    }
  }
  
  case class HMSet(key: String, fieldValuePairs: (String, Any)*) extends Request[Unit](
    HMSet, key, unpair(fieldValuePairs): _*
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class HScan[A, CC[X] <: Traversable[X]](
    key: String,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(
    implicit parser: Parser[A], cbf: CanBuildFrom[Nothing, (String, A), CC[(String, A)]]
  ) extends Request[(Long, CC[(String, A)])](
    HScan,
    generateScanLikeArgs(
      keyOpt = Some(key),
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    )
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[(String, A), CC] {
        case a: ArrayResponse => a.parsedAsPairs[String, A, CC] {
          case b: BulkStringResponse => b.flattened[String]
        } {
          case b: BulkStringResponse => b.flattened[A]
        }
      }
    }
  }
  
  case class HSet(key: String, field: String, value: Any) extends Request[Boolean](
    HSet, key, field, value
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HSetNX(key: String, field: String, value: Any) extends Request[Boolean](
    HSetNX, key, field, value
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HVals[A, CC[X] <: Traversable[X]](key: String)(
    implicit parser: Parser[A], cbf: CanBuildFrom[Nothing, A, CC[A]]
  ) extends Request[CC[A]](HVals, key) {
    override def decode = {
      case a: ArrayResponse => a.parsed[A, CC] {
        case b: BulkStringResponse => b.flattened[A]
      }
    }
  }
  
}