package scredis.protocol

import com.typesafe.scalalogging.Logging
import com.codahale.metrics._

import akka.actor.ActorRef

import scredis.exceptions._
import scredis.util.BufferPool

import scala.collection.mutable.{ ArrayBuilder, ListBuffer }
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Future, Promise }

import java.nio.{ ByteBuffer, CharBuffer }
import java.util.concurrent.Semaphore

/**
 * This object implements various aspects of the `Redis` protocol.
 */
object Protocol {
  
  private[scredis] val metrics = new MetricRegistry()
  private val reporter = JmxReporter.forRegistry(metrics).build()
  
  private val Encoding = "UTF-8"
    
  private val CrByte = '\r'.toByte
  private val CfByte = '\n'.toByte
  private val SimpleStringReplyByte = '+'.toByte
  private val ErrorReplyByte = '-'.toByte
  private val IntegerReplyByte = ':'.toByte
  private val BulkStringReplyByte = '$'.toByte
  private val BulkStringReplyLength = 1
  private val ArrayReplyByte = '*'.toByte
  private val ArrayReplyLength = 1
  
  private val CrLf = "\r\n".getBytes(Encoding)
  private val CrLfLength = CrLf.length
  
  private val bufferPool = new BufferPool(50000)
  private val concurrentOpt: Option[(Semaphore, Boolean)] = Some(new Semaphore(30000), true)
  
  private def aquire(): Unit = concurrentOpt.foreach {
    case (semaphore, true) => semaphore.acquire()
    case (semaphore, false) => if (!semaphore.tryAcquire()) {
      throw new Exception("Busy")
    }
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
  
  private def decodeBulkStringReply(buffer: ByteBuffer): BulkStringReply = {
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
    BulkStringReply(valueOpt)
  }
  
  private[scredis] def release(): Unit = concurrentOpt.foreach {
    case (semaphore, _) => semaphore.release()
  }
  
  private[scredis] def releaseBuffer(buffer: ByteBuffer): Unit = bufferPool.release(buffer)
  
  private[scredis] def encode(command: String): ByteBuffer = encode(Seq[Any](command))
  
  private[scredis] def encode(args: Seq[Any]): ByteBuffer = {
    val argsSize = args.size.toString.getBytes(Encoding)
    
    var length = ArrayReplyLength + argsSize.length + CrLfLength
    
    val serializedArgs = args.map { arg =>
      val serializedArg = arg match {
        case x: Array[Byte] => x
        case x => arg.toString.getBytes(Encoding)
      }
      val serializedArgSize = serializedArg.length.toString.getBytes(Encoding)
      length += BulkStringReplyLength +
        serializedArgSize.length +
        CrLfLength +
        serializedArg.length +
        CrLfLength
      (serializedArg, serializedArgSize)
    }
    
    val buffer = bufferPool.acquire(length)
    
    buffer.put(BulkStringReplyByte).put(argsSize).put(CrLf)
    for ((serializedArg, serializedArgSize) <- serializedArgs) {
      buffer.put(BulkStringReplyByte).put(serializedArgSize).put(CrLf).put(serializedArg).put(CrLf)
    }
    
    buffer.flip()
    buffer
  }
  
  private[scredis] def count(buffer: ByteBuffer): Int = {
    var char: Byte = 0
    var requests = 0
    var arrayCount = 0
    var position = -1
    var arrayPosition = -1
    var isFragmented = false
    
    @inline
    def incr(): Unit = if (arrayCount > 0) {
      arrayCount -= 1
      if (arrayCount == 0) {
        requests += 1
        arrayPosition = -1
      }
    } else {
      requests += 1
    }
    
    @inline
    def fail(): Unit = isFragmented = true
    
    while (buffer.remaining > 0 && !isFragmented) {
      char = buffer.get()
      if (char == ErrorReplyByte || char == SimpleStringReplyByte || char == IntegerReplyByte) {
        position = buffer.position - 1
        while (buffer.remaining > 0 && char != '\n') {
          char = buffer.get()
        }
        if (char == '\n') {
          incr()
        } else {
          fail()
        }
      } else if (char == BulkStringReplyByte) {
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
      } else if (char == ArrayReplyByte) {
        arrayPosition = buffer.position - 1
        try {
          val length = parseInteger(buffer)
          if (length <= 0) {
            incr()
          } else {
            arrayCount = length
          }
        } catch {
          case e: java.nio.BufferUnderflowException => fail()
        }
      }
    }
    
    if (isFragmented) {
      if (arrayPosition >= 0) {
        buffer.position(arrayPosition)
      } else {
        buffer.position(position)
      }
    }
    
    requests
  }
  
  private[scredis] def decode(buffer: ByteBuffer): Reply = buffer.get() match {
    case ErrorReplyByte         => ErrorReply(parseString(buffer))
    case SimpleStringReplyByte  => SimpleStringReply(parseString(buffer))
    case IntegerReplyByte       => IntegerReply(parseLong(buffer))
    case BulkStringReplyByte    => decodeBulkStringReply(buffer)
    case ArrayReplyByte         => ArrayReply(parseInteger(buffer), buffer)
  }
  
  private[scredis] def send[A](request: Request[A])(implicit targetActor: ActorRef): Future[A] = {
    aquire()
    targetActor ! request
    request.future
  }
  
  reporter.start()
  
}

/**
 * This trait represents an instance of a client connected to a `Redis` server.
 */
// TODO: rename this, e.g. ClientLike? Connection? AbstractClient?
trait Protocol extends Logging {
  
  protected implicit val ec = ExecutionContext.global

  protected def flattenKeyValueMap[K <: Any, V <: Any](
    before: List[String], map: Map[K, V]
  ): List[Any] = {
    val keyValues = collection.mutable.MutableList[Any]()
    keyValues ++= before
    for ((key, value) <- map) keyValues += key += value
    keyValues.toList
  }

  protected def flattenValueKeyMap[K <: Any, V <: Any](
    before: List[String], map: Map[K, V]
  ): List[Any] = {
    val keyValues = collection.mutable.MutableList[Any]()
    keyValues ++= before
    for ((key, value) <- map) keyValues += value += key
    keyValues.toList
  }
  
  protected def generateScanLikeArgs(
    name: String,
    keyOpt: Option[String],
    cursor: Long,
    countOpt: Option[Int],
    matchOpt: Option[String]
  ): Seq[Any] = {
    val args = ListBuffer[Any](name)
    keyOpt.foreach { key =>
      args += key
    }
    args += cursor
    countOpt.foreach { count =>
      args += "COUNT"
      args += count
    }
    matchOpt.foreach { pattern =>
      args += "MATCH"
      args += pattern
    }
    args.toList
  }
  
  protected def send[A](request: Request[A]): Future[A] = {
    logger.debug(s"Sending request: $request")
    null
  }
  
}