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

object Get2 extends NullaryCommand("GET") {
  val encoded = NioProtocol.encode(Seq(Get.name, "key"))
}

case class Get2[A](
  implicit parser: Parser[A]
) extends NullaryRequest[Option[A]](Get2) {
  
  
  override private[scredis] def encoded: ByteBuffer = Get2.encoded
  
  protected def decode = {
    case b: BulkReply => b.asX[A]
  }
  
}