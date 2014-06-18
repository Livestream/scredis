package scredis.protocol

import scredis.exceptions._

import scala.util.{ Try, Success, Failure }
import scala.concurrent.Promise

import java.nio.ByteBuffer

abstract class Request[A](command: Command, args: Any*) {
  private val promise = Promise[A]()
  private var _encoded: ByteBuffer = null
  val future = promise.future
  
  private[scredis] def encode(): Unit = {
    _encoded = command.encode(args.toList)
  }
  
  private[scredis] def encoded: ByteBuffer = _encoded
  
  private[scredis] def complete(reply: Reply): Unit = reply match {
    case ErrorReply(error) => promise.failure(RedisCommandException(error))
    case reply => try {
      promise.success(decode(reply))
    } catch {
      case e: Throwable => promise.failure(RedisProtocolException(s"Unexpected reply: $reply", e))
    }
  }
  
  private[scredis] def failure(throwable: Throwable): Unit = {
    promise.failure(throwable)
  }
  
  def decode: PartialFunction[Reply, A]
  def hasArguments = args.size > 0
  
}
