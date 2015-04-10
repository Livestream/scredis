package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object StringRequests {
  
  import scredis.serialization.Implicits.stringReader
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
  
  case class Append[W: Writer](key: String, value: W) extends Request[Long](
    Append, key, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class BitCount(
    key: String, start: Long, end: Long
  ) extends Request[Long](BitCount, key, start, end) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class BitOp(
    operation: scredis.BitOp, destKey: String, keys: String*
  ) extends Request[Long](BitOp, operation.name +: destKey +: keys: _*) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }

    override val key: String = destKey
  }
  
  case class BitPos(
    key: String, bit: Boolean, start: Long, end: Long
  ) extends Request[Long](
    BitPos,
    key,
    if (bit) 1 else 0,
    start,
    end
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Decr(key: String) extends Request[Long](Decr, key) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class DecrBy(key: String, value: Long) extends Request[Long](DecrBy, key, value) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Get[R: Reader](key: String) extends Request[Option[R]](Get, key) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class GetBit(key: String, offset: Long) extends Request[Boolean](GetBit, key, offset) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class GetRange[R: Reader](key: String, start: Long, end: Long) extends Request[R](
    GetRange, key, start, end
  ) with Key {
    override def decode = {
      case b: BulkStringResponse => b.flattened[R]
    }
  }
  
  case class GetSet[R: Reader, W: Writer](key: String, value: W) extends Request[Option[R]](
    GetSet, key, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class Incr(key: String) extends Request[Long](Incr, key) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class IncrBy(key: String, value: Long) extends Request[Long](IncrBy, key, value) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class IncrByFloat(key: String, value: Double) extends Request[Double](
    IncrByFloat, key, value
  ) with Key {
    override def decode = {
      case b: BulkStringResponse => b.flattened[Double]
    }
  }
  
  case class MGet[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](MGet, keys: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }

    override val key: String = keys.head
  }
  
  case class MGetAsMap[R: Reader](keys: String*) extends Request[Map[String, R]](MGet, keys: _*) with Key {
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

    override val key: String = keys.head
  }
  
  case class MSet[W: Writer](keyValuePairs: Map[String, W]) extends Request[Unit](
    MSet,
    unpair(
      keyValuePairs.map {
        case (key, value) => (key, implicitly[Writer[W]].write(value))
      }
    ): _*
  ) with Key  {
    override def decode = {
      case s: SimpleStringResponse => ()
    }

    override val key: String = keyValuePairs.head._1
  }
  
  case class MSetNX[W: Writer](keyValuePairs: Map[String, W]) extends Request[Boolean](
    MSetNX,
    unpair(
      keyValuePairs.map {
        case (key, value) => (key, implicitly[Writer[W]].write(value))
      }
    ): _*
  ) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }

    override val key: String = keyValuePairs.head._1
  }
  
  case class PSetEX[W: Writer](key: String, ttlMillis: Long, value: W) extends Request[Unit](
    PSetEX, key, ttlMillis, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Set[W](
    key: String,
    value: W,
    ttlOpt: Option[FiniteDuration],
    conditionOpt: Option[scredis.Condition]
  )(implicit writer: Writer[W]) extends Request[Boolean](
    Set, key +: writer.write(value) +: {
      val args = ListBuffer[Any]()
      ttlOpt.foreach { ttl =>
        args += "PX" += ttl.toMillis
      }
      conditionOpt.foreach { condition =>
        args += condition.name
      }
      args.toList
    }: _*
  ) with Key {
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
  ) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SetEX[W: Writer](key: String, ttlSeconds: Int, value: W) extends Request[Unit](
    SetEX, key, ttlSeconds, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SetNX[W: Writer](key: String, value: W) extends Request[Boolean](
    SetNX, key, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SetRange[W: Writer](key: String, offset: Long, value: W) extends Request[Long](
    SetRange, key, offset, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }
  
  case class StrLen(key: String) extends Request[Long](StrLen, key) with Key {
    override def decode = {
      case IntegerResponse(length) => length
    }
  }

}