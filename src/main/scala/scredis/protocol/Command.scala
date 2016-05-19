package scredis.protocol

import java.nio.ByteBuffer

abstract class Command(names: String*) {
  def encode(args: List[Any]): ByteBuffer = Protocol.encode(names.toList ::: args)
  def isReadOnly: Boolean = true
  override def toString = names.mkString(" ")
}

abstract class ZeroArgCommand(names: String*) extends Command(names: _*) {
  private[protocol] val encoded: Array[Byte] = Protocol.encodeZeroArgCommand(names.toSeq)
}

trait WriteCommand extends Command {
  override def isReadOnly: Boolean = false
}
