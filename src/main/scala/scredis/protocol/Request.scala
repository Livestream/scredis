package scredis.protocol

import akka.util.ByteString

import scredis.exceptions._
import scredis.serialization.UTF8StringReader

import scala.util.{ Try, Success, Failure }
import scala.concurrent.Promise

import java.nio.ByteBuffer

abstract class Request[A](command: Command, args: Any*) {
  private val promise = Promise[A]()
  private var _buffer: ByteBuffer = null
  private var _bytes: Array[Byte] = null
  val future = promise.future
  val repliesCount = 1
  
  private[scredis] def encode(): Unit = if (_buffer == null && _bytes == null) {
    command match {
      case x: ZeroArgCommand  => _bytes = x.encoded
      case _                  => _buffer = command.encode(args.toList)
    }
  }
  
  private[scredis] def encoded: Either[Array[Byte], ByteBuffer] = if (_bytes != null) {
    Left(_bytes)
  } else {
    Right(_buffer)
  }
  
  private[scredis] def complete(response: Response): Unit = {
    response match {
      case SimpleStringResponse("QUEUED") =>
      case ErrorResponse(message) => failure(RedisErrorResponseException(message))
      case response => try {
        success(decode(response))
      } catch {
        case e: RedisReaderException => failure(e)
        case e: Throwable => failure(
          RedisProtocolException(s"Unexpected response for request '$this': $response", e)
        )
      }
    }
  }
  
  private[scredis] def success(value: Any): Unit = {
    try {
      promise.success(value.asInstanceOf[A])
      Protocol.release()
    } catch {
      case e: IllegalStateException =>
    }
  }
  
  private[scredis] def failure(throwable: Throwable): Unit = {
    try {
      promise.failure(throwable)
      Protocol.release()
    } catch {
      case e: IllegalStateException =>
    }
  }
  
  def decode: Decoder[A]
  
  def argsCount: Int = args.size
  def isReadOnly: Boolean = command.isReadOnly
  
  override def toString = (command +: args).map {
    case bytes: Array[Byte] => UTF8StringReader.read(bytes)
    case x                  => x.toString
  }.mkString(" ")
  
}
