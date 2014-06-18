package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

import scala.util.Try

object Multi extends NullaryCommand("MULTI")

case class Multi() extends Request[Unit](Multi) {
  override def decode = {
    case m: SimpleStringReply => 
  }
}

object Exec extends NullaryCommand("EXEC")

case class Exec(
  parsers: Traversable[PartialFunction[Reply, Any]]
) extends Request[IndexedSeq[Try[Any]]](Multi) {
  override def decode = {
    case a: ArrayReply => a.parsed[IndexedSeq](parsers)
  }
}