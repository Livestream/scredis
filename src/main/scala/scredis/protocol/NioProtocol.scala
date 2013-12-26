package scredis.protocol

import akka.actor.ActorRef

import scredis.util.BufferPool

import scala.concurrent.{ Future, Promise }

import java.nio.{ ByteBuffer, CharBuffer }

object NioProtocol {
  
  private val Encoding = "UTF-8"
    
  private val CrByte = '\r'.toByte
  private val CfByte = '\n'.toByte
  private val StatusReplyByte = '+'.toByte
  private val ErrorReplyByte = '-'.toByte
  private val IntegerReplyByte = ':'.toByte
  private val BulkReplyByte = '$'.toByte
  private val BulkReplyLength = 1
  private val MultiBulkReplyByte = '*'.toByte
  private val MultiBulkReplyLength = 1
  
  private val CrLf = "\r\n".getBytes(Encoding)
  private val CrLfLength = CrLf.length
  
  val bufferPool = new BufferPool(50000)
  
  def encode(command: String): ByteBuffer = encode(Seq[Any](command))
  
  def encode(args: Seq[Any]): ByteBuffer = {
    val argsSize = args.size.toString.getBytes(Encoding)
    
    var length = MultiBulkReplyLength + argsSize.length + CrLfLength
    
    val serializedArgs = args.map { arg =>
      val serializedArg = arg.toString.getBytes(Encoding)
      val serializedArgSize = serializedArg.length.toString.getBytes(Encoding)
      length += BulkReplyLength +
        serializedArgSize.length +
        CrLfLength +
        serializedArg.length +
        CrLfLength
      (serializedArg, serializedArgSize)
    }
    
    val buffer = bufferPool.acquire(length)
    
    buffer.put(MultiBulkReplyByte).put(argsSize).put(CrLf)
    for ((serializedArg, serializedArgSize) <- serializedArgs) {
      buffer.put(BulkReplyByte).put(serializedArgSize).put(CrLf).put(serializedArg).put(CrLf)
    }
    
    buffer.flip()
    buffer
  }
  
  def parseLength(buffer: ByteBuffer): Int = {
    var length: Int = 0
    
    var char = buffer.get()
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    
    // skip \n
    buffer.get()
    length
  }
  
  private def decodeBulkReply(buffer: ByteBuffer): BulkReply = {
    val length = parseLength(buffer)
    val valueOpt = if (length > 0) {
      val array = new Array[Byte](length)
      buffer.get(array)
      buffer.get()
      buffer.get()
      Some(array)
    } else {
      None
    }
    BulkReply(valueOpt)
  }
  
  def decode(buffer: ByteBuffer): Reply = buffer.get() match {
    case ErrorReplyByte     => ErrorReply(buffer)
    case StatusReplyByte    => StatusReply(buffer)
    case IntegerReplyByte   => IntegerReply(buffer)
    case BulkReplyByte      => decodeBulkReply(buffer)
    case MultiBulkReplyByte => MultiBulkReply(parseLength(buffer), buffer)
  }
  
  def send[A](request: Request[A])(implicit encoderActor: ActorRef): Future[A] = {
    encoderActor ! request
    request.future
  }
  
}