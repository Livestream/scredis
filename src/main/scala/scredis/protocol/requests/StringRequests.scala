package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{Reader, Writer}

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object StringRequests {

  import scredis.serialization.Implicits.doubleReader

  object Append extends Command("APPEND") with WriteCommand
  object BitCount extends Command("BITCOUNT")
  object BitOp extends Command("BITOP") with WriteCommand
  object BitPos extends Command("BITPOS")
  object Decr extends Command("DECR") with WriteCommand
  object DecrBy extends Command("DECRBY") with WriteCommand
  object Get extends Command("GET")
  object GetBit extends Command("GETBIT")
  object GetRange extends Command("GETRANGE")
  object GetSet extends Command("GETSET") with WriteCommand
  object Incr extends Command("INCR") with WriteCommand
  object IncrBy extends Command("INCRBY") with WriteCommand
  object IncrByFloat extends Command("INCRBYFLOAT") with WriteCommand
  object MGet extends Command("MGET")
  object MSet extends Command("MSET") with WriteCommand
  object MSetNX extends Command("MSETNX") with WriteCommand
  object PSetEX extends Command("PSETEX") with WriteCommand
  object Set extends Command("SET") with WriteCommand
  object SetBit extends Command("SETBIT") with WriteCommand
  object SetEX extends Command("SETEX") with WriteCommand
  object SetNX extends Command("SETNX") with WriteCommand
  object SetRange extends Command("SETRANGE") with WriteCommand
  object StrLen extends Command("STRLEN")

  case class Append[K, W: Writer](key: K, value: W)(implicit keyWriter: Writer[K]) extends Request[Long](
    Append, keyWriter.write(key), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class BitCount[K](
    key: K, start: Long, end: Long
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    BitCount, keyWriter.write(key), start, end
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class BitOp[KD, K](
    operation: scredis.BitOp, destKey: KD, keys: K*
  )(implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]) extends Request[Long](
    BitOp, operation.name +: destKeyWriter.write(destKey) +: keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class BitPos[K](
    key: K, bit: Boolean, start: Long, end: Long
  )(implicit keyWriter: Writer[K]) extends Request[Long](
    BitPos,
    keyWriter.write(key),
    if (bit) 1 else 0,
    start,
    end
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class Decr[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    Decr, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class DecrBy[K](key: K, value: Long)(implicit keyWriter: Writer[K]) extends Request[Long](
    DecrBy, keyWriter.write(key), value
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class Get[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    Get, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }

  case class GetBit[K](key: K, offset: Long)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    GetBit, keyWriter.write(key), offset
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }

  case class GetRange[K, R: Reader](key: K, start: Long, end: Long)(implicit keyWriter: Writer[K]) extends Request[R](
    GetRange, keyWriter.write(key), start, end
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[R]
    }
  }

  case class GetSet[K, R: Reader, W: Writer](key: K, value: W)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    GetSet, keyWriter.write(key), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }

  case class Incr[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    Incr, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class IncrBy[K](key: K, value: Long)(implicit keyWriter: Writer[K]) extends Request[Long](
    IncrBy, keyWriter.write(key), value
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }

  case class IncrByFloat[K](key: K, value: Double)(implicit keyWriter: Writer[K]) extends Request[Double](
    IncrByFloat, keyWriter.write(key), value
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }

  case class MGet[K, R: Reader, CC[X] <: Traversable[X]](keys: K*)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](
    MGet, keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }
  }

  case class MGetAsMap[K, R: Reader](keys: K*)(implicit keyWriter: Writer[K]) extends Request[Map[K, R]](
    MGet, keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case a: ArrayResponse => {
        val values = a.parsed[Option[R], List] {
          case b: BulkStringResponse => b.parsed[R]
        }
        keys.zip(values).flatMap {
          case (key, Some(value)) => Some((key, value))
          case _ => None
        }.toMap
      }
    }
  }

  case class MSet[K, W: Writer](keyValuePairs: Map[K, W])(implicit keyWriter: Writer[K]) extends Request[Unit](
    MSet,
    unpair(
      keyValuePairs.map {
        case (key, value) => (keyWriter.write(key), implicitly[Writer[W]].write(value))
      }
    ): _*
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }

  case class MSetNX[K, W: Writer](keyValuePairs: Map[K, W])(implicit keyWriter: Writer[K]) extends Request[Boolean](
    MSetNX,
    unpair(
      keyValuePairs.map {
        case (key, value) => (keyWriter.write(key), implicitly[Writer[W]].write(value))
      }
    ): _*
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }

  case class PSetEX[K, W: Writer](
    key: K, ttlMillis: Long, value: W
  )(implicit keyWriter: Writer[K]) extends Request[Unit](
    PSetEX, keyWriter.write(key), ttlMillis, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }

  case class Set[K, W](
    key: K,
    value: W,
    ttlOpt: Option[FiniteDuration],
    conditionOpt: Option[scredis.Condition]
  )(
    implicit keyWriter: Writer[K], writer: Writer[W]
  ) extends Request[Boolean](
    Set, keyWriter.write(key) +: writer.write(value) +: {
      val args = ListBuffer[Any]()
      ttlOpt.foreach { ttl =>
        args += "PX" += ttl.toMillis
      }
      conditionOpt.foreach { condition =>
        args += condition.name
      }
      args.toList
    }: _*
  ) {
    override def decode = {
      case SimpleStringResponse(_)  => true
      case BulkStringResponse(None) => false
    }
  }

  case class SetBit[K](key: K, offset: Long, value: Boolean)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    SetBit,
    keyWriter.write(key),
    offset,
    if (value) 1 else 0
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }

  case class SetEX[K, W: Writer](key: K, ttlSeconds: Int, value: W)(implicit keyWriter: Writer[K]) extends Request[Unit](
    SetEX, keyWriter.write(key), ttlSeconds, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class SetNX[K, W: Writer](key: K, value: W)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    SetNX, keyWriter.write(key), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }

  case class SetRange[K, W: Writer](key: K, offset: Long, value: W)(implicit keyWriter: Writer[K]) extends Request[Long](
    SetRange, keyWriter.write(key), offset, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }

  case class StrLen[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    StrLen, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }

}