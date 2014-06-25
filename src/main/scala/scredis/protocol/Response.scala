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

case class IntegerResponse(value: Long) extends Response {
  def toBoolean: Boolean = value > 0
}

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
      val response = Protocol.decode(buffer)
      if (parsePf.isDefinedAt(response)) {
        builder += parsePf.apply(response)
      } else {
        throw new IllegalArgumentException(s"Does not know how to parse response: $response")
      }
      i += 1
    }
    builder.result()
  }
  
  def parsedAsPairs[A, B, CC[X] <: Traversable[X]](
    parseFirstPf: PartialFunction[Response, A]
  )(
    parseSecondPf: PartialFunction[Response, B]
  )(implicit cbf: CanBuildFrom[Nothing, (A, B), CC[(A, B)]]): CC[(A, B)] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val firstResponse = Protocol.decode(buffer)
      val firstValue = if (parseFirstPf.isDefinedAt(firstResponse)) {
        parseFirstPf.apply(firstResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse first response: $firstResponse"
        )
      }
      val secondResponse = Protocol.decode(buffer)
      val secondValue = if (parseSecondPf.isDefinedAt(secondResponse)) {
        parseSecondPf.apply(secondResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse second response: $secondResponse"
        )
      }
      i += 2
    }
    builder.result()
  }
  
  def parsedAsPairsMap[A, B, CC[X, Y] <: collection.Map[X, Y]](
    parseFirstPf: PartialFunction[Response, A]
  )(
    parseSecondPf: PartialFunction[Response, B]
  )(implicit cbf: CanBuildFrom[Nothing, (A, B), CC[A, B]]): CC[A, B] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val firstResponse = Protocol.decode(buffer)
      val firstValue = if (parseFirstPf.isDefinedAt(firstResponse)) {
        parseFirstPf.apply(firstResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse first response: $firstResponse"
        )
      }
      val secondResponse = Protocol.decode(buffer)
      val secondValue = if (parseSecondPf.isDefinedAt(secondResponse)) {
        parseSecondPf.apply(secondResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse second response: $secondResponse"
        )
      }
      i += 2
    }
    builder.result()
  }
  
  def parsedAsScanResponse[A, CC[X] <: Traversable[X]](
    parsePf: PartialFunction[Response, CC[A]]
  ): (Long, CC[A]) = {
    if (length != 2) {
      throw RedisProtocolException(s"Unexpected length for scan-like array response: $length")
    }
    
    val nextCursor = Protocol.decode(buffer) match {
      case IntegerResponse(cursor) => cursor
      case x => throw RedisProtocolException(s"Unexpected response for scan cursor: $x")
    }
    
    Protocol.decode(buffer) match {
      case a: ArrayResponse if parsePf.isDefinedAt(a) => (nextCursor, parsePf.apply(a))
      case a: ArrayResponse => throw new IllegalArgumentException(
        s"Does not know how to parse response: $a"
      )
      case x => throw RedisProtocolException(s"Unexpected response for scan elements: $x")
    }
  }
  
  def parsed[CC[X] <: Traversable[X]](parsers: Traversable[PartialFunction[Response, Any]])(
    implicit cbf: CanBuildFrom[Nothing, Try[Any], CC[Try[Any]]]
  ): CC[Try[Any]] = {
    val builder = cbf()
    var i = 0
    val parsersIterator = parsers.toIterator
    while (i < length) {
      val response = Protocol.decode(buffer)
      val parser = parsersIterator.next()
      val result = response match {
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