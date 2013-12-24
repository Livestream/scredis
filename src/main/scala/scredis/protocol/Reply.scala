package scredis.protocol

import scala.collection.mutable.ListBuffer

import java.nio.ByteBuffer

trait Reply {
  def asAny: Any
}

case class ErrorReply(buffer: ByteBuffer) extends Reply {
  def asAny: Any = asString
  def asString: String = buffer.toString
}

case class StatusReply(buffer: ByteBuffer) extends Reply {
  def asAny: Any = asString
  def asString: String = buffer.toString
}

case class IntegerReply(buffer: ByteBuffer) extends Reply {
  def asAny: Any = asLong
  def asString: String = buffer.toString
  def asInt: Int = asString.toInt
  def asLong: Long = asString.toLong
  def asBoolean: Boolean = (asInt == 1)
}

case class BulkReply(length: Int, value: ByteBuffer) extends Reply {
  def asByteArrayOpt: Option[Array[Byte]] = if (length > 0) {
    val length = value.remaining - 2
    val array = new Array[Byte](length)
    value.get(array)
    Some(array)
  } else {
    None
  }
  def asByteArray = asByteArrayOpt.get
  
  def asAny: Any = asByteArrayOpt
  def asStringOpt: Option[String] = asByteArrayOpt.map(new String(_, "UTF-8"))
  def asIntOpt: Option[Int] = asStringOpt.map(_.toInt)
  def asLongOpt: Option[Long] = asStringOpt.map(_.toLong)
  def asBooleanOpt: Option[Boolean] = asIntOpt.map(_ == 1)
}

case class MultiBulkReply(length: Int, buffer: ByteBuffer) extends Reply {
  
  private val replies = {
    val list = ListBuffer[Reply]()
    var i = 0
    while (i < length) {
      list += NioProtocol.decode(buffer)
      i += 1
    }
    list.toList
  }
  
  def asAny: List[Any] = replies.map(_.asAny)
  
}