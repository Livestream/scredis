package scredis.protocol

import java.nio.ByteBuffer

import akka.util.ByteString
import scredis.{Server, ClusterSlotRange}
import scredis.exceptions._
import scredis.serialization.Reader

import scala.collection.generic.CanBuildFrom
import scala.util.{Failure, Success, Try}

sealed trait Response

case class ErrorResponse(value: String) extends Response

case class SimpleStringResponse(value: String) extends Response

case class IntegerResponse(value: Long) extends Response {
  def toBoolean: Boolean = value > 0
}

/** Errors specific to cluster operation */
sealed trait ClusterError
case class Moved(hashSlot: Int, host: String, port: Int) extends ClusterError
case class Ask(hashSlot: Int, host: String, port: Int) extends ClusterError
case object TryAgain extends ClusterError
case object ClusterDown extends ClusterError
case object CrossSlot extends ClusterError

case class ClusterErrorResponse(error: ClusterError, message: String) extends Response

case class BulkStringResponse(valueOpt: Option[Array[Byte]]) extends Response {
  def parsed[R](implicit reader: Reader[R]): Option[R] = valueOpt.map(reader.read)

  def flattened[R](implicit reader: Reader[R]): R = parsed[R].get

  override def toString = s"BulkStringResponse(" +
    s"${valueOpt.map(ByteString(_).decodeString("UTF-8"))})"
}

case class ArrayResponse(length: Int, buffer: ByteBuffer) extends Response {

  def headOpt[R](decoder: Decoder[R]): Option[R] = if (length > 0) {
    val position = buffer.position
    val response = Protocol.decode(buffer)
    if (decoder.isDefinedAt(response)) {
      val decoded = decoder.apply(response)
      buffer.position(position)
      Some(decoded)
    } else {
      throw new IllegalArgumentException(s"Does not know how to parse response: $response")
    }
  } else {
    None
  }

  def parsed[R, CC[X] <: Traversable[X]](decoder: Decoder[R])(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
    ): CC[R] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val response = Protocol.decode(buffer)
      if (decoder.isDefinedAt(response)) {
        builder += decoder.apply(response)
      } else {
        throw new IllegalArgumentException(s"Does not know how to parse response: $response")
      }
      i += 1
    }
    builder.result()
  }

  def parsedAsPairs[R1, R2, CC[X] <: Traversable[X]](
    firstDecoder: Decoder[R1]
  )(
    secondDecoder: Decoder[R2]
  )(implicit cbf: CanBuildFrom[Nothing, (R1, R2), CC[(R1, R2)]]): CC[(R1, R2)] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val firstResponse = Protocol.decode(buffer)
      val firstValue = if (firstDecoder.isDefinedAt(firstResponse)) {
        firstDecoder.apply(firstResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse first response: $firstResponse"
        )
      }
      val secondResponse = Protocol.decode(buffer)
      val secondValue = if (secondDecoder.isDefinedAt(secondResponse)) {
        secondDecoder.apply(secondResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse second response: $secondResponse"
        )
      }
      builder += ((firstValue, secondValue))
      i += 2
    }
    builder.result()
  }

  def parsedAsPairsMap[R1, R2, CC[X, Y] <: collection.Map[X, Y]](
    firstDecoder: Decoder[R1]
  )(
    secondDecoder: Decoder[R2]
  )(implicit cbf: CanBuildFrom[Nothing, (R1, R2), CC[R1, R2]]): CC[R1, R2] = {
    val builder = cbf()
    var i = 0
    while (i < length) {
      val firstResponse = Protocol.decode(buffer)
      val firstValue = if (firstDecoder.isDefinedAt(firstResponse)) {
        firstDecoder.apply(firstResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse first response: $firstResponse"
        )
      }
      val secondResponse = Protocol.decode(buffer)
      val secondValue = if (secondDecoder.isDefinedAt(secondResponse)) {
        secondDecoder.apply(secondResponse)
      } else {
        throw new IllegalArgumentException(
          s"Does not know how to parse second response: $secondResponse"
        )
      }
      builder += ((firstValue, secondValue))
      i += 2
    }
    builder.result()
  }

  def parsedAsScanResponse[R, CC[X] <: Traversable[X]](
    decoder: Decoder[CC[R]]
  ): (Long, CC[R]) = {
    if (length != 2) {
      throw RedisProtocolException(s"Unexpected length for scan-like array response: $length")
    }

    val nextCursor = Protocol.decode(buffer) match {
      case b: BulkStringResponse => b.flattened[String].toLong
      case x => throw RedisProtocolException(s"Unexpected response for scan cursor: $x")
    }

    Protocol.decode(buffer) match {
      case a: ArrayResponse if decoder.isDefinedAt(a) => (nextCursor, decoder.apply(a))
      case a: ArrayResponse => throw new IllegalArgumentException(
        s"Does not know how to parse response: $a"
      )
      case x => throw RedisProtocolException(s"Unexpected response for scan elements: $x")
    }
  }

  def parsedAsClusterSlotsResponse[CC[X] <: Traversable[X]](
    implicit cbf: CanBuildFrom[Nothing, ClusterSlotRange, CC[ClusterSlotRange]]) : CC[ClusterSlotRange] = {
    val builder = cbf()
    var i = 0
    try {
      while (i < length) {
        Protocol.decode(buffer) match {
          case a: ArrayResponse =>
            val begin = Protocol.decode(buffer).asInstanceOf[IntegerResponse].value
            val end = Protocol.decode(buffer).asInstanceOf[IntegerResponse].value

            // master for this slotrange
            Protocol.decode(buffer).asInstanceOf[ArrayResponse] // mast host/port header
            val masterHost = Protocol.decode(buffer).asInstanceOf[BulkStringResponse].parsed[String].get
            val masterPort = Protocol.decode(buffer).asInstanceOf[IntegerResponse].value



            var r = 3 // first replica begins at index 3
            var replicaList: List[Server] = Nil
            while (r < a.length) {
              Protocol.decode(buffer).asInstanceOf[ArrayResponse] // replica header
              val replicaHost = Protocol.decode(buffer).asInstanceOf[BulkStringResponse].parsed[String].get
              val replicaPort = Protocol.decode(buffer).asInstanceOf[IntegerResponse].value
              replicaList = Server(replicaHost, replicaPort.toInt) :: replicaList
              r += 1
            }

            builder += ClusterSlotRange((begin, end), Server(masterHost, masterPort.toInt), replicaList)

          case other => throw RedisProtocolException(s"Expected an array parsing CLUSTER SLOTS reply, got $other")
        }

        i += 1
      }
    } catch {
      case rpe: RedisProtocolException => throw rpe
      case x: Exception => throw RedisProtocolException("Unexpected values parsing CLUSTER SLOTS reply", x)
    }

    builder.result()
  }

  def parsed[CC[X] <: Traversable[X]](decoders: Traversable[Decoder[Any]])(
    implicit cbf: CanBuildFrom[Nothing, Try[Any], CC[Try[Any]]]
    ): CC[Try[Any]] = {
    val builder = cbf()
    var i = 0
    val decodersIterator = decoders.toIterator
    while (i < length) {
      val response = Protocol.decode(buffer)
      val decoder = decodersIterator.next()
      val result = response match {
        case ErrorResponse(message) => Failure(RedisErrorResponseException(message))
        case ClusterErrorResponse(error, message) => Failure(RedisClusterErrorResponseException(error, message))
        case reply => if (decoder.isDefinedAt(reply)) {
          try {
            Success(decoder.apply(reply))
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

  override def toString = s"ArrayResponse(length=$length, buffer=" +
    s"${ByteString(buffer).decodeString("UTF-8")})"

}