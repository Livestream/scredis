package scredis.protocol

import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Future, Promise }

import java.nio.ByteBuffer

trait Command[A] {
  
  private val promise = Promise[A]()
  
  private[scredis] final val encoded: ByteBuffer = encode()
  
  val name: String
  val future: Future[A] = promise.future
  
  private def encode() = if (args.isEmpty) {
    NioProtocol.multiBulkRequestFor(name)
  } else {
    NioProtocol.multiBulkRequestFor(name, args)
  }
  
  protected def args: Seq[Any]
  protected def decode: PartialFunction[Reply, A]
  
  private[scredis] def complete(reply: Try[Reply]): Unit = reply match {
    case Success(s @ ErrorReply(_)) => promise.failure(new Exception(s.asString))
    case Success(reply) => promise.success(decode(reply))
    case Failure(e) => promise.failure(e)
  }
  
}

trait NullaryCommand[A] extends Command[A] {
  protected final def args = Seq.empty[Any]
}