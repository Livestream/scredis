package scredis.protocol.commands

import scredis.protocol._

import java.nio.ByteBuffer

object Ping extends NullaryCommand("PING")

case class Ping() extends Request[String](Ping) {
  override def decode = {  
    case StatusReply(value) => value
  }
}