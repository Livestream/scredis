package scredis.protocol

import scredis.serialization.Writer

import scala.collection.mutable.ListBuffer

package object requests {
  
  private[requests] def generateScanLikeArgs[K](
    keyOpt: Option[K],
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(implicit keyWriter: Writer[K]): List[Any] = {
    val args = ListBuffer[Any]()
    keyOpt.foreach { key =>
      args += keyWriter.write(key)
    }
    args += cursor
    countOpt.foreach {
      args += "COUNT" += _
    }
    matchOpt.foreach {
      args += "MATCH" += _
    }
    args.toList
  }
  
  private[requests] def unpair[A, B, CC[X] <: Traversable[X]](pairs: CC[(A, B)]): List[Any] = {
    val unpaired = ListBuffer[Any]()
    pairs.foreach {
      case (a, b) => unpaired += a += b
    }
    unpaired.toList
  }
  
}