package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object ListRequests {
  
  import scredis.serialization.Implicits.stringReader
  
  private object BLPop extends Command("BLPOP")
  private object BRPop extends Command("BRPOP")
  private object BRPopLPush extends Command("BRPOPLPUSH")
  private object LIndex extends Command("LINDEX")
  private object LInsert extends Command("LINSERT")
  private object LLen extends Command("LLEN")
  private object LPop extends Command("LPOP")
  private object LPush extends Command("LPUSH")
  private object LPushX extends Command("LPUSHX")
  private object LRange extends Command("LRANGE")
  private object LRem extends Command("LREM")
  private object LSet extends Command("LSET")
  private object LTrim extends Command("LTRIM")
  private object RPop extends Command("RPOP")
  private object RPopLPush extends Command("RPOPLPUSH")
  private object RPush extends Command("RPUSH")
  private object RPushX extends Command("RPUSHX")
  
  case class BLPop[R: Reader](
    timeoutSeconds: Int, keys: String*
  ) extends Request[Option[(String, R)]](BLPop, keys: _*, timeoutSeconds) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
  }
  
  case class BRPop[R: Reader](
    timeoutSeconds: Int, keys: String*
  ) extends Request[Option[(String, R)]](BRPop, keys: _*, timeoutSeconds) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
  }
  
  case class BRPopLPush[R: Reader](
    source: String, destination: String, timeoutSeconds: Int
  ) extends Request[Option[R]](BRPopLPush, source, destination, timeoutSeconds) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LIndex[R: Reader](key: String, index: Long) extends Request[Option[R]](
    LIndex, key, index
  ) {
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
  ) {
    override def decode = {
      case IntegerResponse(-1)  => None
      case IntegerResponse(x)   => Some(x)
    }
  }
  
  case class LLen(key: String) extends Request[Long](LLen, key) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPop[R: Reader](key: String) extends Request[Option[R]](LPop, key) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LPush[W: Writer](key: String, values: W*) extends Request[Long](
    LPush, key, values.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPushX[W: Writer](key: String, value: W) extends Request[Long](
    LPushX, key, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LRange[R: Reader, CC[X] <: Traversable[X]](key: String, start: Long, end: Long)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](LRange, key, start, end) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class LRem[W: Writer](key: String, count: Int, value: W) extends Request[Long](
    LRem, key, count, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LSet[W: Writer](key: String, index: Long, value: W) extends Request[Unit](
    LSet, key, index, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class LTrim(key: String, start: Long, end: Long) extends Request[Unit](
    LTrim, key, start, end
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class RPop[R: Reader](key: String) extends Request[Option[R]](RPop, key) {
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
  
  case class RPush[W: Writer](key: String, values: W*) extends Request[Long](
    RPush, key, values.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class RPushX[W: Writer](key: String, value: W) extends Request[Long](
    RPushX, key, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
}