package scredis.protocol.requests

import scredis.protocol._
import scredis.parsing.Parser

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object StringRequests {
  
  import scredis.parsing.Implicits.stringParser
  import scredis.parsing.Implicits.doubleParser
  
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
  private object MGet extends ZeroArgCommand("MGET")
  private object MSet extends Command("MSET")
  private object MSetNX extends Command("MSETNX")
  private object PSetEX extends Command("PSETEX")
  private object Set extends Command("SET")
  private object SetBit extends Command("SETBIT")
  private object SetEX extends Command("SETEX")
  private object SetNX extends Command("SETNX")
  private object SetRange extends Command("SETRANGE")
  private object StrLen extends Command("STRLEN")
  
  case class Append(key: String, value: Any) extends Request[Long](Append, key, value) {
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
  
  case class Get[A](key: String)(implicit parser: Parser[A]) extends Request[Option[A]](Get, key) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[A]
    }
  }
  
  case class GetBit(key: String, offset: Long) extends Request[Boolean](GetBit, key, offset) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class GetRange[A](key: String, start: Long, end: Long)(
    implicit parser: Parser[A]
  ) extends Request[A](GetRange, key, start, end) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[A]
    }
  }
  
  case class GetSet[A](key: String, value: Any)(
    implicit parser: Parser[A]
  ) extends Request[Option[A]](GetSet, key, value) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[A]
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
  
  case class MGet[A, CC[X] <: Traversable[X]](keys: String*)(
    implicit parser: Parser[A], cbf: CanBuildFrom[Nothing, Option[A], CC[Option[A]]]
  ) extends Request[CC[Option[A]]](MGet, keys: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[A], CC] {
        case b: BulkStringResponse => b.parsed[A]
      }
    }
  }
  
  case class MGetAsMap[A](keys: String*)(
    implicit parser: Parser[A]
  ) extends Request[Map[String, A]](MGet, keys: _*) {
    override def decode = {
      case a: ArrayResponse => {
        val values = a.parsed[Option[A], List] {
          case b: BulkStringResponse => b.parsed[A]
        }
        keys.zip(values).flatMap {
          case (key, Some(value)) => Some((key, value))
          case _ => None
        }.toMap
      }
    }
  }
  
  case class MSet(keyValuePairs: (String, Any)*) extends Request[Unit](
    MSet, unpair(keyValuePairs): _*
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class MSetNX(keyValuePairs: (String, Any)*) extends Request[Boolean](
    MSetNX, unpair(keyValuePairs): _*
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PSetEX(key: String, ttlMillis: Long, value: Any) extends Request[Unit](
    PSetEX, key, ttlMillis, value
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Set(
    key: String,
    value: Any,
    ttlOpt: Option[FiniteDuration],
    conditionOpt: Option[scredis.Condition]
  ) extends Request[Boolean](
    Set,
    {
      val args = ListBuffer[Any](key, value)
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
  
  case class SetEX(key: String, ttlSeconds: Int, value: Any) extends Request[Unit](
    SetEX, key, ttlSeconds, value
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SetNX(key: String, value: Any) extends Request[Boolean](
    SetNX, key, value
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SetRange(key: String, offset: Long, value: Any) extends Request[Long](
    SetRange, key, offset, value
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