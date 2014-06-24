package scredis.protocol

import scredis.exceptions._
import scredis.parsing.Parser

import scala.util.{ Try, Success, Failure }
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer

trait Response

case class ErrorResponse(value: String) extends Response

case class SimpleStringResponse(value: String) extends Response

case class IntegerResponse(value: Long) extends Response

case class BulkStringResponse(valueOpt: Option[Array[Byte]]) extends Response {
  def parsed[A](implicit parser: Parser[A]): Option[A] = valueOpt.map(parser.parse)
  def flattened[A](implicit parser: Parser[A]): A = parsed[A].get
}

case class ArrayResponse(length: Int, buffer: ByteBuffer) extends Response {
  
  def parsed[A, CC[X] <: Traversable[X]](parsePf: PartialFunction[Response, A])(
    implicit cbf: CanBuildFrom[Nothing, A, CC[A]]
  ): CC[A] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val reply = Protocol.decode(buffer)
      if (parsePf.isDefinedAt(reply)) {
        builder += parsePf.apply(reply)
      } else {
        throw new IllegalArgumentException(s"Does not know how to parse reply: $reply")
      }
      i += 1
    }
    builder.result()
  }
  
  def parsed[CC[X] <: Traversable[X]](parsers: Traversable[PartialFunction[Response, Any]])(
    implicit cbf: CanBuildFrom[Nothing, Try[Any], CC[Try[Any]]]
  ): CC[Try[Any]] = {
    val builder = cbf()
    var i = 0
    val parsersIterator = parsers.toIterator
    while (i < length) {
      val reply = Protocol.decode(buffer)
      val parser = parsersIterator.next()
      val result = reply match {
        case ErrorResponse(message) => Failure(RedisErrorResponseException(message))
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