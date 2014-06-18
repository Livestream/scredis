package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

object LRange extends Command("LRANGE")

case class LRange[A](key: String, from: Long, to: Long)(
  implicit parser: Parser[A]
) extends Request[List[A]](LRange, key, from, to) {
  override def decode = {
    case a: ArrayReply => a.parsed[A, List] {
      case b: BulkStringReply => b.flattened[A]
    }
  }
}