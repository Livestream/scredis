package scredis.protocol

import java.nio.CharBuffer

object ReplyDecoder {
  
  
  
  def decodeBulkReply[A](buffer: CharBuffer)(implicit decoder: CharBuffer => A): Option[A] = {
    val `type` = buffer.get()
    
    var char = buffer.get()
    // negative or zero
    if (char == '-' || char == '0') return None
    
    var length: Int = 0
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    // skip \n
    buffer.position(buffer.position + 2)
    
    Some(decoder(buffer))
  }
  
}