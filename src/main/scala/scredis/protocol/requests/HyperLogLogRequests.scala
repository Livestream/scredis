package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.Writer

object HyperLogLogRequests {
  
  object PFAdd extends Command("PFADD")
  object PFCount extends Command("PFCOUNT")
  object PFMerge extends Command("PFMERGE")
  
  case class PFAdd[W: Writer](key: String, elements: W*) extends Request[Boolean](
    PFAdd, key, elements.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {  
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PFCount(keys: String*) extends Request[Long](PFCount, keys: _*) {
    override def decode = {  
      case IntegerResponse(value) => value
    }
  }
  
  case class PFMerge(destination: String, keys: String*) extends Request[Unit](
    PFMerge, destination, keys: _*
  ) {
    override def decode = {  
      case SimpleStringResponse(_) => ()
    }
  }

}