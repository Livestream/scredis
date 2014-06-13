package scredis.protocol

import scredis.parsing.Parser

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer

trait Reply

case class ErrorReply(value: String) extends Reply

case class StatusReply(value: String) extends Reply

case class IntegerReply(value: Long) extends Reply

case class BulkReply(valueOpt: Option[Array[Byte]]) extends Reply {
  def parsed[A](implicit parser: Parser[A]): Option[A] = valueOpt.map(parser.parse)
  def flattened[A](implicit parser: Parser[A]): A = parsed[A].get
}

case class MultiBulkReply(length: Int, buffer: ByteBuffer) extends Reply {
  
  def parsed[A, CC[X] <: Traversable[X]](parsePf: PartialFunction[Reply, A])(
    implicit cbf: CanBuildFrom[Nothing, A, CC[A]]
  ): CC[A] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val reply = NioProtocol.decode(buffer)
      if (parsePf.isDefinedAt(reply)) {
        builder += parsePf.apply(reply)
      } else {
        throw new IllegalArgumentException(s"Does not know how to parse reply: $reply")
      }
      i += 1
    }
    builder.result()
  }
  
}