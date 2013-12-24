package scredis.protocol

import scala.util.{ Try, Success, Failure }
import scala.concurrent.Promise

import java.nio.ByteBuffer

abstract class Request[A, B](command: Command[A], args: List[Any]) {
  private val promise = Promise[B]()
  private var _encoded: ByteBuffer = null
  val future = promise.future
  
  protected def transform(value: A): B
  
  private[scredis] def encode(): Unit = {
    _encoded = command.apply(args)
  }
  
  private[scredis] def encoded: ByteBuffer = _encoded
  private[scredis] def isEncoded: Boolean = (_encoded != null)
  
  private[scredis] def complete(reply: Try[Reply]): Unit = reply match {
    case Success(s @ ErrorReply(_)) => promise.failure(new Exception(s.asString))
    case Success(reply) => try {
      promise.success(transform(command.parse(reply)))
    } catch {
      case e: Throwable => promise.failure(e)
    }
    case Failure(e) => promise.failure(e)
  }
  
}

case class SimpleRequest[A](
  command: Command[A], args: List[Any]
) extends Request[A, A](command, args) {
  protected def transform(value: A): A = value
}

case class TransformableRequest[A, B](command: Command[A], args: List[Any])(
  f: A => B
) extends Request[A, B](command, args) {
  protected def transform(value: A): B = f(value)
}