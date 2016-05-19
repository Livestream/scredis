package scredis.protocol.requests

import scala.language.higherKinds
import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object ListRequests {
  
  import scredis.serialization.Implicits.stringReader
  
  object BLPop extends Command("BLPOP") with WriteCommand
  object BRPop extends Command("BRPOP") with WriteCommand
  object BRPopLPush extends Command("BRPOPLPUSH") with WriteCommand
  object LIndex extends Command("LINDEX")
  object LInsert extends Command("LINSERT") with WriteCommand
  object LLen extends Command("LLEN")
  object LPop extends Command("LPOP") with WriteCommand
  object LPush extends Command("LPUSH") with WriteCommand
  object LPushX extends Command("LPUSHX") with WriteCommand
  object LRange extends Command("LRANGE")
  object LRem extends Command("LREM") with WriteCommand
  object LSet extends Command("LSET") with WriteCommand
  object LTrim extends Command("LTRIM") with WriteCommand
  object RPop extends Command("RPOP") with WriteCommand
  object RPopLPush extends Command("RPOPLPUSH") with WriteCommand
  object RPush extends Command("RPUSH") with WriteCommand
  object RPushX extends Command("RPUSHX") with WriteCommand
  
  case class BLPop[R: Reader](
    timeoutSeconds: Int, keys: String*
  ) extends Request[Option[(String, R)]](BLPop, keys :+ timeoutSeconds: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
    override val key = keys.head
  }
  
  case class BRPop[R: Reader](
    timeoutSeconds: Int, keys: String*
  ) extends Request[Option[(String, R)]](BRPop, keys :+ timeoutSeconds: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
    override val key = keys.head
  }
  
  case class BRPopLPush[R: Reader](
    source: String, destination: String, timeoutSeconds: Int
  ) extends Request[Option[R]](BRPopLPush, source, destination, timeoutSeconds) {
    override def decode = {
      case b: BulkStringResponse  => b.parsed[R]
      case ArrayResponse(-1, _)   => None
    }
  }
  
  case class LIndex[R: Reader](key: String, index: Long) extends Request[Option[R]](
    LIndex, key, index
  ) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LInsert[W1: Writer, W2: Writer](
    key: String, position: scredis.Position, pivot: W1, value: W2
  ) extends Request[Option[Long]](
    LInsert,
    key,
    position.name,
    implicitly[Writer[W1]].write(pivot),
    implicitly[Writer[W2]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(-1)  => None
      case IntegerResponse(x)   => Some(x)
    }
  }
  
  case class LLen(key: String) extends Request[Long](LLen, key) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPop[R: Reader](key: String) extends Request[Option[R]](LPop, key) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LPush[W](key: String, values: W*)(implicit writer: Writer[W]) extends Request[Long](
    LPush, key +: values.map(writer.write): _*
  ) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPushX[W: Writer](key: String, value: W) extends Request[Long](
    LPushX, key, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LRange[R: Reader, CC[X] <: Traversable[X]](key: String, start: Long, end: Long)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](LRange, key, start, end) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class LRem[W: Writer](key: String, count: Int, value: W) extends Request[Long](
    LRem, key, count, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LSet[W: Writer](key: String, index: Long, value: W) extends Request[Unit](
    LSet, key, index, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class LTrim(key: String, start: Long, end: Long) extends Request[Unit](
    LTrim, key, start, end
  ) with Key {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class RPop[R: Reader](key: String) extends Request[Option[R]](RPop, key) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class RPopLPush[R: Reader](
    source: String, destination: String
  ) extends Request[Option[R]](RPopLPush, source, destination) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class RPush[W](key: String, values: W*)(implicit writer: Writer[W]) extends Request[Long](
    RPush, key +: values.map(writer.write): _*
  ) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class RPushX[W: Writer](key: String, value: W) extends Request[Long](
    RPushX, key, implicitly[Writer[W]].write(value)
  ) with Key {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
}