package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.Writer

object HyperLogLogRequests {
  
  object PFAdd extends Command("PFADD") with WriteCommand
  object PFCount extends Command("PFCOUNT")
  object PFMerge extends Command("PFMERGE") with WriteCommand
  
  case class PFAdd[K, W](key: K, elements: W*)(
    implicit keyWriter: Writer[K], writer: Writer[W]
  ) extends Request[Boolean](
    PFAdd, keyWriter.write(key) +: elements.map(writer.write): _*
  ) {
    override def decode = {  
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PFCount[K](keys: K*)(implicit keyWriter: Writer[K]) extends Request[Long](
    PFCount, keys.map(keyWriter.write): _*
  ) {
    override def decode = {  
      case IntegerResponse(value) => value
    }
  }
  
  case class PFMerge[KD, K](destination: KD, keys: K*)(
    implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Unit](
    PFMerge, destKeyWriter.write(destination) +: keys.map(keyWriter.write): _*
  ) {
    override def decode = {  
      case SimpleStringResponse(_) => ()
    }
  }
  
}