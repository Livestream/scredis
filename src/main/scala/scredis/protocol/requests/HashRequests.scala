package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object HashRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.doubleReader
  
  object HDel extends Command("HDEL") with WriteCommand
  object HExists extends Command("HEXISTS")
  object HGet extends Command("HGET")
  object HGetAll extends Command("HGETALL")
  object HIncrBy extends Command("HINCRBY") with WriteCommand
  object HIncrByFloat extends Command("HINCRBYFLOAT") with WriteCommand
  object HKeys extends Command("HKEYS")
  object HLen extends Command("HLEN")
  object HMGet extends Command("HMGET")
  object HMSet extends Command("HMSET") with WriteCommand
  object HScan extends Command("HSCAN")
  object HSet extends Command("HSET") with WriteCommand
  object HSetNX extends Command("HSETNX") with WriteCommand
  object HVals extends Command("HVALS")
  
  case class HDel(key: String, fields: String*) extends Request[Long](
    HDel, key +: fields: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HExists(key: String, field: String) extends Request[Boolean](HExists, key, field) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HGet[R: Reader](key: String, field: String) extends Request[Option[R]](
    HGet, key, field
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class HGetAll[R: Reader](key: String) extends Request[Map[String, R]](HGetAll, key) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, R, Map] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
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
  
  case class HKeys[CC[X] <: Traversable[X]](key: String)(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
  ) extends Request[CC[String]](HKeys, key) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class HLen(key: String) extends Request[Long](HLen, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HMGet[R: Reader, CC[X] <: Traversable[X]](key: String, fields: String*)(
    implicit cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](HMGet, key +: fields: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }
  }
  
  case class HMGetAsMap[R: Reader](key: String, fields: String*) extends Request[Map[String, R]](
    HMGet, key +: fields: _*
  ) {
    override def decode = {
      case a: ArrayResponse => {
        val values = a.parsed[Option[R], List] {
          case b: BulkStringResponse => b.parsed[R]
        }
        fields.zip(values).flatMap {
          case (key, Some(value)) => Some((key, value))
          case _ => None
        }.toMap
      }
    }
  }
  
  case class HMSet[W](key: String, fieldValuePairs: (String, W)*)(
    implicit writer: Writer[W]
  ) extends Request[Unit](
    HMSet,
    key :: unpair(
      fieldValuePairs.map {
        case (field, value) => (field, writer.write(value))
      }
    ): _*
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class HScan[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(
    implicit cbf: CanBuildFrom[Nothing, (String, R), CC[(String, R)]]
  ) extends Request[(Long, CC[(String, R)])](
    HScan,
    generateScanLikeArgs(
      keyOpt = Some(key),
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    ): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[(String, R), CC] {
        case a: ArrayResponse => a.parsedAsPairs[String, R, CC] {
          case b: BulkStringResponse => b.flattened[String]
        } {
          case b: BulkStringResponse => b.flattened[R]
        }
      }
    }
  }
  
  case class HSet[W: Writer](key: String, field: String, value: W) extends Request[Boolean](
    HSet, key, field, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HSetNX[W: Writer](key: String, field: String, value: W) extends Request[Boolean](
    HSetNX, key, field, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HVals[R: Reader, CC[X] <: Traversable[X]](key: String)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](HVals, key) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
}