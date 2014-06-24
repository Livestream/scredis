package scredis.protocol

import scredis.parsing.Parser

import scala.util.Try
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

package object commands {
  
  private[commands] def generateScanLikeArgs(
    keyOpt: Option[String],
    cursor: Long,
    countOpt: Option[Int],
    matchOpt: Option[String]
  ): List[Any] = {
    val args = ListBuffer[Any]()
    keyOpt.foreach { key =>
      args += key
    }
    args += cursor
    countOpt.foreach { count =>
      args += "COUNT"
      args += count
    }
    matchOpt.foreach { pattern =>
      args += "MATCH"
      args += pattern
    }
    args.toList
  }
  
}