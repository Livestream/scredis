package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

object Get extends Command("GET")

case class Get[A](key: String)(
  implicit parser: Parser[A]
) extends Request[Option[A]](Get, key) {
  override def decode = {
    case b: BulkStringResponse => b.parsed[A]
  }
}