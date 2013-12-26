package scredis.protocol

import java.nio.ByteBuffer

abstract class Command(val name: String) {
  def encode(args: List[Any]): ByteBuffer = NioProtocol.encode(name :: args)
}

abstract class NullaryCommand(name: String) extends Command(name) {
  private val encoded = NioProtocol.encode(name)
  override def encode(args: List[Any]): ByteBuffer = encoded
}