package scredis.protocol.commands

import scredis.protocol._

import java.nio.ByteBuffer


case class Auth(password: String) extends Command[Unit] {
  val name = "PING"
  
  protected def args = Seq(password)
  
  protected def decode = {
    case StatusReply(_) => ()
  }
  
}

object Ping extends NullaryCommand[String] {
  val name = "PING"
  
  protected def decode = {
    case s @ StatusReply(_) => s.asString
  }
  
}