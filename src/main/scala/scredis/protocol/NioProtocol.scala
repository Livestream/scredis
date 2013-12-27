package scredis.protocol

import com.codahale.metrics._

import akka.actor.ActorRef

import scredis.util.BufferPool

import scala.concurrent.{ Future, Promise }

import java.nio.{ ByteBuffer, CharBuffer }

object NioProtocol {
  
  private[scredis] val metrics = new MetricRegistry()
  private val reporter = JmxReporter.forRegistry(metrics).build()
  
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
  
  def count(buffer: ByteBuffer): Int = {
    var char: Byte = 0
    var requests = 0
    var multiBulkCount = 0
    var position = -1
    var multiBulkPosition = -1
    var isFragmented = false
    
    def incr(): Unit = if (multiBulkCount > 0) {
      multiBulkCount -= 1
      if (multiBulkCount == 0) {
        requests += 1
        multiBulkPosition = -1
      }
    } else {
      requests += 1
    }
    
    def fail(): Unit = isFragmented = true
    
    while (buffer.remaining > 0 && !isFragmented) {
      char = buffer.get()
      if (char == ErrorReplyByte || char == StatusReplyByte || char == IntegerReplyByte) {
        position = buffer.position - 1
        while (buffer.remaining > 0 && char != '\n') {
          char = buffer.get()
        }
        if (char == '\n') {
          incr()
        } else {
          fail()
        }
      } else if (char == BulkReplyByte) {
        position = buffer.position - 1
        try {
          val length = parseLength(buffer) + 2
          if (buffer.remaining >= length) {
            buffer.position(buffer.position + length)
            incr()
          } else {
            fail()
          }
        } catch {
          case e: java.nio.BufferUnderflowException => fail()
        }
      } else if (char == MultiBulkReplyByte) {
        multiBulkPosition = buffer.position - 1
        try {
          multiBulkCount = parseLength(buffer)
        } catch {
          case e: java.nio.BufferUnderflowException => fail()
        }
      }
    }
    
    if (isFragmented) {
      if (multiBulkPosition >= 0) {
        buffer.position(multiBulkPosition)
      } else {
        buffer.position(position)
      }
    }
    requests
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
  
  reporter.start()
  
}