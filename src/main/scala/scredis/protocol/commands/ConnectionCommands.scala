package scredis.protocol.commands

import scredis.protocol._

object Ping extends NullaryCommand("PING")

case class Ping() extends Request[String](Ping) {
  override def decode = {  
    case SimpleStringReply(value) => value
  }
}