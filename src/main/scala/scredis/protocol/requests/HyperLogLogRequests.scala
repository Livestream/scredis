package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.Writer

object HyperLogLogRequests {
  
  object PFAdd extends Command("PFADD") with WriteCommand
  object PFCount extends Command("PFCOUNT")
  object PFMerge extends Command("PFMERGE") with WriteCommand
  
  case class PFAdd[W](key: String, elements: W*)(
    implicit writer: Writer[W]
  ) extends Request[Boolean](PFAdd, key +: elements.map(writer.write): _*) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PFCount(keys: String*) extends Request[Long](PFCount, keys: _*) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
    override val key = keys.head
  }
  
  case class PFMerge(destination: String, keys: String*) extends Request[Unit](
    PFMerge, destination +: keys: _*
  ) with Key {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }

    override val key = keys.head
  }
  
}