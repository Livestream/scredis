package scredis.protocol.commands

import scredis.protocol._

import java.nio.ByteBuffer


object Get extends Command[Option[Array[Byte]]]("GET") {
  
  protected def decode = {
    case b @ BulkReply(_, _) => b.asByteArrayOpt
  }
  
}