package scredis.protocol


import java.nio.{ ByteBuffer, CharBuffer }

trait Reply {
  def asString: String
}

case class ErrorReply(buffer: CharBuffer) extends Reply {
  override def asString: String = buffer.toString
}

case class StatusReply(buffer: CharBuffer) extends Reply {
  override def asString: String = buffer.toString
}

case class IntegerReply(buffer: CharBuffer) extends Reply {
  
  private def parseInt(): Int = {
    var char = buffer.get()
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
  }
  
  override def asString: String = buffer.toString
  def asInt: Int = 
  def asBoolean: Boolean = (asInt == 1)
}

case class BulkReply(value: Option[CharBuffer]) extends Reply {
  override def asString: String = value.toString
  def toBoolean: Boolean = (value == 1)
}


case object BulkReply {
  
  def decode[A](buffer: CharBuffer)(implicit decoder: CharBuffer => A): Option[A] = {
    val `type` = buffer.get()
    var length: Int = 0
    
    var char = buffer.get()
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    if (length <= 0) return None
    
    // skip \n
    buffer.position(buffer.position + 2)
    
    Some(decoder(buffer))
  }
  
}

