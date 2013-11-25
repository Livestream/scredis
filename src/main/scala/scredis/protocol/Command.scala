package scredis.protocol

import java.nio.ByteBuffer

trait Command[R <: Reply[T], T] {
  
  val name: String
  val encoded: ByteBuffer
  val decoder: ByteBuffer => T
  
  final protected def encode(): ByteBuffer = {
    NioProtocol.multiBulkRequestFor(name)
  }
  
  final protected def encode(args: Seq[Any]): ByteBuffer = {
    NioProtocol.multiBulkRequestFor(name, args)
  }
  
}