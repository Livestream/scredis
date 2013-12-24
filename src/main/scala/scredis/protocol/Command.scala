package scredis.protocol

import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Future, Promise }

import java.nio.ByteBuffer

abstract class Command[A](name: String) {
  
  protected def decode: PartialFunction[Reply, A]
  
  private[scredis] def parse(reply: Reply): A = decode(reply)
  
  private[scredis] def apply(args: List[Any]): ByteBuffer = {
    NioProtocol.encode(name :: args)
  }
  
}

abstract class NullaryCommand[A](name: String) extends Command[A](name) {
  private val encoded = NioProtocol.encode(name)
  override def apply(args: List[Any]): ByteBuffer = encoded
}