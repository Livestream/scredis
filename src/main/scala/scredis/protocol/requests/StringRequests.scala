package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object StringRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.doubleReader
  
  private object Append extends Command("APPEND")
  private object BitCount extends Command("BITCOUNT")
  private object BitOp extends Command("BITOP")
  private object BitPos extends Command("BITPOS")
  private object Decr extends Command("DECR")
  private object DecrBy extends Command("DECRBY")
  private object Get extends Command("GET")
  private object GetBit extends Command("GETBIT")
  private object GetRange extends Command("GETRANGE")
  private object GetSet extends Command("GETSET")
  private object Incr extends Command("INCR")
  private object IncrBy extends Command("INCRBY")
  private object IncrByFloat extends Command("INCRBYFLOAT")
  private object MGet extends Command("MGET")
  private object MSet extends Command("MSET")
  private object MSetNX extends Command("MSETNX")
  private object PSetEX extends Command("PSETEX")
  private object Set extends Command("SET")
  private object SetBit extends Command("SETBIT")
  private object SetEX extends Command("SETEX")
  private object SetNX extends Command("SETNX")
  private object SetRange extends Command("SETRANGE")
  private object StrLen extends Command("STRLEN")
  
  case class Append[W: Writer](key: String, value: W) extends Request[Long](
    Append, key, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class BitCount(
    key: String, start: Long, end: Long
  ) extends Request[Long](BitCount, key, start, end) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class BitOp(
    operation: scredis.BitOp, destKey: String, keys: String*
  ) extends Request[Long](BitOp, operation.name, destKey, keys: _*) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class BitPos(
    key: String, bit: Boolean, start: Long, end: Long
  ) extends Request[Long](
    BitPos,
    key,
    if (bit) 1 else 0,
    start,
    end
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Decr(key: String) extends Request[Long](Decr, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class DecrBy(key: String, value: Long) extends Request[Long](DecrBy, key, value) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Get[R: Reader](key: String) extends Request[Option[R]](Get, key) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class GetBit(key: String, offset: Long) extends Request[Boolean](GetBit, key, offset) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class GetRange[R: Reader](key: String, start: Long, end: Long) extends Request[R](
    GetRange, key, start, end
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[R]
    }
  }
  
  case class GetSet[R: Reader, W: Writer](key: String, value: W) extends Request[Option[R]](
    GetSet, key, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class Incr(key: String) extends Request[Long](Incr, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class IncrBy(key: String, value: Long) extends Request[Long](IncrBy, key, value) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class IncrByFloat(key: String, value: Double) extends Request[Double](
    IncrByFloat, key, value
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class MGet[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](MGet, keys: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }
  }
  
  case class MGetAsMap[R: Reader](keys: String*) extends Request[Map[String, R]](MGet, keys: _*) {
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
  
  case class MSet[W: Writer](keyValuePairs: (String, W)*) extends Request[Unit](
    MSet,
    unpair(
      keyValuePairs.map {
        case (key, value) => (key, implicitly[Writer[W]].write(value))
      }
    ): _*
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class MSetNX[W: Writer](keyValuePairs: (String, W)*) extends Request[Boolean](
    MSetNX,
    unpair(
      keyValuePairs.map {
        case (key, value) => (key, implicitly[Writer[W]].write(value))
      }
    ): _*
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PSetEX[W: Writer](key: String, ttlMillis: Long, value: W) extends Request[Unit](
    PSetEX, key, ttlMillis, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Set[W: Writer](
    key: String,
    value: W,
    ttlOpt: Option[FiniteDuration],
    conditionOpt: Option[scredis.Condition]
  ) extends Request[Boolean](
    Set,
    key,
    implicitly[Writer[W]].write(value),
    {
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
  
  case class SetBit(key: String, offset: Long, value: Boolean) extends Request[Boolean](
    SetBit,
    key,
    offset,
    if (value) 1 else 0
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SetEX[W: Writer](key: String, ttlSeconds: Int, value: W) extends Request[Unit](
    SetEX, key, ttlSeconds, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SetNX[W: Writer](key: String, value: W) extends Request[Boolean](
    SetNX, key, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SetRange[W: Writer](key: String, offset: Long, value: W) extends Request[Long](
    SetRange, key, offset, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }
  
  case class StrLen(key: String) extends Request[Long](StrLen, key) {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }

}