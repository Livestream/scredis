package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

import java.nio.ByteBuffer


object Get extends Command("GET")

case class Get[A](key: String)(
  implicit parser: Parser[A]
) extends Request[Option[A]](Get, List(key)) {
  
  protected def decode = {
    case b: BulkReply => b.asX[A]
  }
  
}