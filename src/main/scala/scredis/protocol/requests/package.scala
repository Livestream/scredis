package scredis.protocol

import scala.collection.mutable.ListBuffer

package object requests {
  
  private[requests] def generateScanLikeArgs(
    keyOpt: Option[String],
    cursor: Long,
    countOpt: Option[Int],
    matchOpt: Option[String]
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
  
}