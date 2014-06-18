package scredis.protocol

import scredis.exceptions._
import scredis.parsing.Parser

import scala.util.{ Try, Success, Failure }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer

trait Reply

case class ErrorReply(value: String) extends Reply

case class SimpleStringReply(value: String) extends Reply

case class IntegerReply(value: Long) extends Reply

case class BulkStringReply(valueOpt: Option[Array[Byte]]) extends Reply {
  def parsed[A](implicit parser: Parser[A]): Option[A] = valueOpt.map(parser.parse)
  def flattened[A](implicit parser: Parser[A]): A = parsed[A].get
}

case class ArrayReply(length: Int, buffer: ByteBuffer) extends Reply {
  
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
  
  def parsed[CC[X] <: Traversable[X]](parsers: Traversable[PartialFunction[Reply, Any]])(
    implicit cbf: CanBuildFrom[Nothing, Try[Any], CC[Try[Any]]]
  ): CC[Try[Any]] = {
    val builder = cbf()
    var i = 0
    val parsersIterator = parsers.toIterator
    while (i < length) {
      val reply = NioProtocol.decode(buffer)
      val parser = parsersIterator.next()
      val result = reply match {
        case ErrorReply(error) => Failure(RedisCommandException(error))
        case reply => if (parser.isDefinedAt(reply)) {
          try {
            Success(parser.apply(reply))
          } catch {
            case e: Throwable => Failure(RedisProtocolException("", e))
          }
        } else {
          Failure(RedisProtocolException(s"Unexpected reply: $reply"))
        }
      }
      builder += result
      i += 1
    }
    builder.result()
  }
  
}