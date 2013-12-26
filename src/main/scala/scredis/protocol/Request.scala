package scredis.protocol

import scala.util.{ Try, Success, Failure }
import scala.concurrent.Promise

import java.nio.ByteBuffer

abstract class Request[A](command: Command, args: List[Any]) {
  private val promise = Promise[A]()
  private var _encoded: ByteBuffer = null
  val future = promise.future
  
  protected def decode: PartialFunction[Reply, A]
  
  private[scredis] def encode(): Unit = {
    _encoded = command.encode(args)
  }
  
  private[scredis] def encoded: ByteBuffer = _encoded
  
  private[scredis] def complete(reply: Try[Reply]): Unit = reply match {
    case Success(s @ ErrorReply(_)) => promise.failure(new Exception(s.asString))
    case Success(reply) => try {
      promise.success(decode(reply))
    } catch {
      case e: Throwable => promise.failure(e)
    }
    case Failure(e) => promise.failure(e)
  }
  
}

abstract class NullaryRequest[A](command: Command) extends Request[A](command, Nil)