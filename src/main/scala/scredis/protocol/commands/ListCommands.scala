package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

import java.nio.ByteBuffer

object LRange extends Command("LRANGE")

case class LRange[A](key: String, from: Long, to: Long)(
  implicit parser: Parser[A]
) extends Request[List[A]](LRange, key, from, to) {
  override def decode = {
    case m: MultiBulkReply => m.parsed[A, List] {
      case b: BulkReply => b.flattened[A]
    }
  }
}