package scredis.protocol.commands

import scredis.protocol._

import java.nio.ByteBuffer

object Auth extends NullaryCommand("AUTH")

case class Auth() extends NullaryRequest[Unit](Auth) {
  
  protected def decode = {
    case StatusReply(_) => Unit
  }
  
}

object Ping extends NullaryCommand("PING")

case class Ping() extends NullaryRequest[String](Ping) {
  
  protected def decode = {
    case s @ StatusReply(_) => s.asString
  }
  
}