package scredis.protocol


import java.nio.{ ByteBuffer, CharBuffer }

trait Reply

case class ErrorReply(buffer: CharBuffer) extends Reply {
  def asString: String = buffer.toString
}

case class StatusReply(buffer: CharBuffer) extends Reply {
  def asString: String = buffer.toString
}

case class IntegerReply(buffer: CharBuffer) extends Reply {  
  def asString: String = buffer.toString
  def asInt: Int = asString.toInt
  def asBoolean: Boolean = (asInt == 1)
}

case class BulkReply(length: Int, value: CharBuffer) extends Reply {
  def asStringOpt: Option[String] = if (length > 0) {
    Some(value.toString)
  } else {
    None
  }
  def asIntOpt: Option[Int] = asStringOpt.map(_.toInt)
  def asLongOpt: Option[Long] = asStringOpt.map(_.toLong)
  def asBooleanOpt: Option[Boolean] = asIntOpt.map(_ == 1)
}


case object BulkReply {
  
  def apply(buffer: CharBuffer): BulkReply = {
    val `type` = buffer.get()
    var length: Int = 0
    
    var char = buffer.get()
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    
    // skip \n
    buffer.position(buffer.position + 2)
    
    BulkReply(length, buffer)
  }
  
}

