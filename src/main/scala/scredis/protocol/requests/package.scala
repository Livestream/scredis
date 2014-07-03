package scredis.protocol

import scala.collection.mutable.ListBuffer

package object requests {
  
  private[requests] def generateScanLikeArgs(
    keyOpt: Option[String],
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  ): List[Any] = {
    val args = ListBuffer[Any]()
    keyOpt.foreach {
      args += _
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