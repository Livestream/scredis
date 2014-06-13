package scredis.protocol

import com.codahale.metrics._

import akka.actor.ActorRef

import scredis.util.BufferPool

import scala.collection.mutable.ArrayBuilder
import scala.concurrent.{ Future, Promise }

import java.nio.{ ByteBuffer, CharBuffer }
import java.util.concurrent.Semaphore

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
  val concurrent = new Semaphore(30000)
  
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
  
  private def parseInteger(buffer: ByteBuffer): Int = {
    var length: Int = 0
    var isPositive = true
    
    var char = buffer.get()
    
    if (char == '-') {
      isPositive = false
      char = buffer.get()
    }
    
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    
    // skip \n
    buffer.get()
    if (isPositive) {
      length
    } else {
      -length
    }
  }
  
  private def parseLong(buffer: ByteBuffer): Long = {
    var length: Long = 0
    var isPositive = true
    
    var char = buffer.get()
    
    if (char == '-') {
      isPositive = false
      char = buffer.get()
    }
    
    while (char != '\r') {
      length = (length * 10) + (char - '0')
      char = buffer.get()
    }
    
    // skip \n
    buffer.get()
    if (isPositive) {
      length
    } else {
      -length
    }
  }
  
  private def parseString(buffer: ByteBuffer): String = {
    val bytes = new ArrayBuilder.ofByte()
    var count = 0
    var char = buffer.get()
    while (char != '\r') {
      bytes += char
      char = buffer.get()
    }
    buffer.get()
    new String(bytes.result(), "UTF-8")
  }
  
  private def decodeBulkReply(buffer: ByteBuffer): BulkReply = {
    val length = parseInteger(buffer)
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
  
  def count(buffer: ByteBuffer): Int = {
    var char: Byte = 0
    var requests = 0
    var multiBulkCount = 0
    var position = -1
    var multiBulkPosition = -1
    var isFragmented = false
    
    @inline
    def incr(): Unit = if (multiBulkCount > 0) {
      multiBulkCount -= 1
      if (multiBulkCount == 0) {
        requests += 1
        multiBulkPosition = -1
      }
    } else {
      requests += 1
    }
    
    @inline
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
          val length = parseInteger(buffer) match {
            case -1 => 0
            case x => x + 2
          }
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
          val length = parseInteger(buffer)
          if (length <= 0) {
            incr()
          } else {
            multiBulkCount = length
          }
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
  
  def decode(buffer: ByteBuffer): Reply = buffer.get() match {
    case ErrorReplyByte     => ErrorReply(parseString(buffer))
    case StatusReplyByte    => StatusReply(parseString(buffer))
    case IntegerReplyByte   => IntegerReply(parseLong(buffer))
    case BulkReplyByte      => decodeBulkReply(buffer)
    case MultiBulkReplyByte => MultiBulkReply(parseInteger(buffer), buffer)
  }
  
  def send[A](request: Request[A])(implicit encoderActor: ActorRef): Future[A] = {
    encoderActor ! request
    request.future
  }
  
  reporter.start()
  
}