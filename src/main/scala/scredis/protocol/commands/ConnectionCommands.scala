package scredis.protocol.commands

import scredis.protocol._

import java.nio.ByteBuffer


object Auth extends Command[Unit]("AUTH") {
  
  protected def decode = {
    case StatusReply(_) => ()
  }
  
}

object Ping extends NullaryCommand[String]("PING") {
  
  protected def decode = {
    case s @ StatusReply(_) => s.asString
  }
  
}