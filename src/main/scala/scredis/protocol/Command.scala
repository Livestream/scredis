package scredis.protocol

import java.nio.ByteBuffer

abstract class Command(val name: String) {
  def encode(args: List[Any]): ByteBuffer = Protocol.encode(name :: args)
  override def toString = name
}

abstract class ZeroArgCommand(name: String) extends Command(name) {
  private[protocol] val encoded: Array[Byte] = Protocol.encode(name)
}