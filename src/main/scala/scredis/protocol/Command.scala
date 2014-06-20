package scredis.protocol

import java.nio.ByteBuffer

abstract class Command(val name: String) {
  def encode(args: List[Any]): ByteBuffer = Protocol.encode(name :: args)
}

abstract class ZeroArgCommand(name: String) extends Command(name) {
  private val encoded = Protocol.encode(name)
  override def encode(args: List[Any]): ByteBuffer = encoded
}