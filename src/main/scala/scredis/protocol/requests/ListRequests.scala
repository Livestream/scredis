package scredis.protocol.requests

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
  
  case class BLPop[K, R: Reader](
    timeoutSeconds: Int, keys: K*
  )(implicit keyWriter: Writer[K]) extends Request[Option[(String, R)]](
    BLPop, keys.map(keyWriter.write) :+ timeoutSeconds: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
  }
  
  case class BRPop[K, R: Reader](
    timeoutSeconds: Int, keys: K*
  )(implicit keyWriter: Writer[K]) extends Request[Option[(String, R)]](
    BRPop, keys.map(keyWriter.write) :+ timeoutSeconds: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairs[String, R, List] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[R]
      }.headOption
    }
  }
  
  case class BRPopLPush[KS, KD, R: Reader](
    source: KS, destination: KD, timeoutSeconds: Int
  )(
    implicit sourceKeyWriter: Writer[KS], destKeyWriter: Writer[KD]
  ) extends Request[Option[R]](
    BRPopLPush, sourceKeyWriter.write(source), destKeyWriter.write(destination), timeoutSeconds
  ) {
    override def decode = {
      case b: BulkStringResponse  => b.parsed[R]
      case ArrayResponse(-1, _)   => None
    }
  }
  
  case class LIndex[K, R: Reader](key: K, index: Long)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    LIndex, keyWriter.write(key), index
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LInsert[K, W1: Writer, W2: Writer](
    key: K, position: scredis.Position, pivot: W1, value: W2
  )(implicit keyWriter: Writer[K]) extends Request[Option[Long]](
    LInsert,
    keyWriter.write(key),
    position.name,
    implicitly[Writer[W1]].write(pivot),
    implicitly[Writer[W2]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(-1)  => None
      case IntegerResponse(x)   => Some(x)
    }
  }
  
  case class LLen[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    LLen, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPop[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    LPop, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class LPush[K, W](key: K, values: W*)(implicit writer: Writer[W], keyWriter: Writer[K]) extends Request[Long](
    LPush, keyWriter.write(key) +: values.map(writer.write): _*
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LPushX[K, W: Writer](key: K, value: W)(implicit keyWriter: Writer[K]) extends Request[Long](
    LPushX, keyWriter.write(key), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LRange[K, R: Reader, CC[X] <: Traversable[X]](key: K, start: Long, end: Long)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    LRange, keyWriter.write(key), start, end
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class LRem[K, W: Writer](key: K, count: Int, value: W)(implicit keyWriter: Writer[K]) extends Request[Long](
    LRem, keyWriter.write(key), count, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class LSet[K, W: Writer](key: K, index: Long, value: W)(implicit keyWriter: Writer[K]) extends Request[Unit](
    LSet, keyWriter.write(key), index, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class LTrim[K](key: K, start: Long, end: Long)(implicit keyWriter: Writer[K]) extends Request[Unit](
    LTrim, keyWriter.write(key), start, end
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class RPop[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    RPop, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class RPopLPush[KS, KD, R: Reader](
    source: KS, destination: KD
  )(
    implicit sourceKeyWriter: Writer[KS], destKeyWriter: Writer[KD]
  ) extends Request[Option[R]](
    RPopLPush, sourceKeyWriter.write(source), destKeyWriter.write(destination)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class RPush[K, W](key: K, values: W*)(implicit writer: Writer[W], keyWriter: Writer[K]) extends Request[Long](
    RPush, keyWriter.write(key) +: values.map(writer.write): _*
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
  case class RPushX[K, W: Writer](key: K, value: W)(implicit keyWriter: Writer[K]) extends Request[Long](
    RPushX, keyWriter.write(key), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case IntegerResponse(x) => x
    }
  }
  
}