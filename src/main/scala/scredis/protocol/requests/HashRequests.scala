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
  
  case class HDel[K](key: K, fields: String*)(implicit keyWriter: Writer[K]) extends Request[Long](
    HDel, keyWriter.write(key) +: fields: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HExists[K](key: K, field: String)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    HExists, keyWriter.write(key), field
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HGet[K, R: Reader](key: K, field: String)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    HGet, keyWriter.write(key), field
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class HGetAll[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Map[String, R]](
    HGetAll, keyWriter.write(key)
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, R, Map] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class HIncrBy[K](key: K, field: String, value: Long)(implicit keyWriter: Writer[K]) extends Request[Long](
    HIncrBy, keyWriter.write(key), field, value
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HIncrByFloat[K](key: K, field: String, value: Double)(implicit keyWriter: Writer[K]) extends Request[Double](
    HIncrByFloat, keyWriter.write(key), field, value
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class HKeys[K, CC[X] <: Traversable[X]](key: K)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, String, CC[String]]
  ) extends Request[CC[String]](
    HKeys, keyWriter.write(key)
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class HLen[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    HLen, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class HMGet[K, R: Reader, CC[X] <: Traversable[X]](key: K, fields: String*)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](
    HMGet, keyWriter.write(key) +: fields: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }
  }
  
  case class HMGetAsMap[K, R: Reader](key: K, fields: String*)(implicit keyWriter: Writer[K]) extends Request[Map[String, R]](
    HMGet, keyWriter.write(key) +: fields: _*
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
  
  case class HMSet[K, W](key: K, fieldValuePairs: (String, W)*)(
    implicit keyWriter: Writer[K], writer: Writer[W]
  ) extends Request[Unit](
    HMSet,
    keyWriter.write(key) :: unpair(
      fieldValuePairs.map {
        case (field, value) => (field, writer.write(value))
      }
    ): _*
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class HScan[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, (String, R), CC[(String, R)]]
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
  
  case class HSet[K, W: Writer](key: K, field: String, value: W)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    HSet, keyWriter.write(key), field, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HSetNX[K, W: Writer](key: K, field: String, value: W)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    HSetNX, keyWriter.write(key), field, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class HVals[K, R: Reader, CC[X] <: Traversable[X]](key: K)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    HVals, keyWriter.write(key)
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
}