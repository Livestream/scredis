package scredis.protocol

import com.typesafe.scalalogging.Logging
import com.codahale.metrics._

import akka.actor.ActorRef
import akka.util.ByteString

import scredis.PubSubMessage
import scredis.exceptions._
import scredis.serialization.UTF8StringReader
import scredis.util.BufferPool

import scala.collection.mutable.{ ArrayBuilder, ListBuffer, Stack }
import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Future, Promise, Await }
import scala.concurrent.duration.Duration
import scala.annotation.tailrec

import java.nio.{ ByteBuffer, CharBuffer }
import java.util.concurrent.Semaphore

import scala.language.higherKinds

/**
 * This object implements various aspects of the `Redis` protocol.
 */
object Protocol {
  
  private case class ArrayState(
    val size: Int,
    var count: Int
  ) {
    def increment(): Unit = count += 1
    def isCompleted = (count == size)
  }
  
  private[scredis] case object IgnoredPubSubMessageException extends Exception
  
  private[scredis] val metrics = new MetricRegistry()
  private val reporter = JmxReporter.forRegistry(metrics).build()
  
  private val Encoding = "UTF-8"
    
  private val CrByte = '\r'.toByte
  private val CfByte = '\n'.toByte
  private val SimpleStringResponseByte = '+'.toByte
  private val ErrorResponseByte = '-'.toByte
  private val IntegerResponseByte = ':'.toByte
  private val BulkStringResponseByte = '$'.toByte
  private val BulkStringResponseLength = 1
  private val ArrayResponseByte = '*'.toByte
  private val ArrayResponseLength = 1
  
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
  
  private def parseInt(buffer: ByteBuffer): Int = {
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
  
  private def decodeBulkStringResponse(buffer: ByteBuffer): BulkStringResponse = {
    val length = parseInt(buffer)
    val valueOpt = if (length >= 0) {
      val array = new Array[Byte](length)
      buffer.get(array)
      buffer.get()
      buffer.get()
      Some(array)
    } else {
      None
    }
    BulkStringResponse(valueOpt)
  }
  
  private[scredis] def release(): Unit = concurrentOpt.foreach {
    case (semaphore, _) => semaphore.release()
  }
  
  private[scredis] def releaseBuffer(buffer: ByteBuffer): Unit = bufferPool.release(buffer)
  
  private[scredis] def encodeZeroArgCommand(names: Seq[String]): Array[Byte] = {
    val buffer = encode(names)
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    bufferPool.release(buffer)
    bytes
  }
  
  private[scredis] def encode(args: Seq[Any]): ByteBuffer = {
    val argsSize = args.size.toString.getBytes(Encoding)
    
    var length = ArrayResponseLength + argsSize.length + CrLfLength
    
    val serializedArgs = args.map { arg =>
      val serializedArg = arg match {
        case null => throw new NullPointerException(args.mkString(" "))
        case x: Array[Byte] => x
        case x => arg.toString.getBytes(Encoding)
      }
      val serializedArgSize = serializedArg.length.toString.getBytes(Encoding)
      length += ArrayResponseLength +
        serializedArgSize.length +
        CrLfLength +
        serializedArg.length +
        CrLfLength
      (serializedArg, serializedArgSize)
    }
    
    val buffer = bufferPool.acquire(length)
    buffer.put(ArrayResponseByte).put(argsSize).put(CrLf)
    for ((serializedArg, serializedArgSize) <- serializedArgs) {
      buffer
        .put(BulkStringResponseByte)
        .put(serializedArgSize)
        .put(CrLf)
        .put(serializedArg)
        .put(CrLf)
    }
    
    buffer.flip()
    buffer
  }
  
  private[scredis] def count(buffer: ByteBuffer): Int = {
    var char: Byte = 0
    var requests = 0
    var position = -1
    var arrayPosition = -1
    val arrayStack = Stack[ArrayState]()
    var isFragmented = false
    
    @inline @tailrec
    def increment(): Unit = if (arrayStack.isEmpty) {
      arrayPosition = -1
      requests += 1
    } else {
      val array = arrayStack.top
      array.increment()
      if (array.isCompleted) {
        arrayStack.pop()
        increment()
      }
    }
    
    @inline
    def stop(): Unit = isFragmented = true
    
    while (buffer.remaining > 0 && !isFragmented) {
      char = buffer.get()
      if (
        char == ErrorResponseByte ||
        char == SimpleStringResponseByte ||
        char == IntegerResponseByte
      ) {
        position = buffer.position - 1
        while (buffer.remaining > 0 && char != '\n') {
          char = buffer.get()
        }
        if (char == '\n') {
          increment()
        } else {
          stop()
        }
      } else if (char == BulkStringResponseByte) {
        position = buffer.position - 1
        try {
          val length = parseInt(buffer) match {
            case -1 => 0
            case x => x + 2
          }
          if (buffer.remaining >= length) {
            buffer.position(buffer.position + length)
            increment()
          } else {
            stop()
          }
        } catch {
          case e: java.nio.BufferUnderflowException => stop()
        }
      } else if (char == ArrayResponseByte) {
        if (arrayStack.isEmpty) {
          arrayPosition = buffer.position - 1
        }
        try {
          val length = parseInt(buffer)
          if (length <= 0) {
            increment()
          } else {
            arrayStack.push(ArrayState(length, 0))
          }
        } catch {
          case e: java.nio.BufferUnderflowException => stop()
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
  
  private[scredis] def decode(buffer: ByteBuffer): Response = buffer.get() match {
    case ErrorResponseByte         => ErrorResponse(parseString(buffer))
    case SimpleStringResponseByte  => SimpleStringResponse(parseString(buffer))
    case IntegerResponseByte       => IntegerResponse(parseLong(buffer))
    case BulkStringResponseByte    => decodeBulkStringResponse(buffer)
    case ArrayResponseByte         => ArrayResponse(parseInt(buffer), buffer)
  }
  
  private[scredis] def decodePubSubResponse(response: Response): PubSubMessage = response match {
    case a: ArrayResponse => {
      val vector = a.parsed[Any, Vector] {
        case BulkStringResponse(valueOpt) => valueOpt
        case IntegerResponse(value) => value.toInt
      }
      val kind = UTF8StringReader.read(vector(0).asInstanceOf[Option[Array[Byte]]].get)
      kind match {
        case "subscribe"    => {
          val channel = UTF8StringReader.read(vector(1).asInstanceOf[Option[Array[Byte]]].get)
          val channelsCount = vector(2).asInstanceOf[Int]
          PubSubMessage.Subscribe(channel, channelsCount)
        }
        case "psubscribe"   => {
          val pattern = UTF8StringReader.read(vector(1).asInstanceOf[Option[Array[Byte]]].get)
          val patternsCount = vector(2).asInstanceOf[Int]
          PubSubMessage.PSubscribe(pattern, patternsCount)
        }
        case "unsubscribe"  => {
          val channelOpt = vector(1).asInstanceOf[Option[Array[Byte]]].map(UTF8StringReader.read)
          val channelsCount = vector(2).asInstanceOf[Int]
          PubSubMessage.Unsubscribe(channelOpt, channelsCount)
        }
        case "punsubscribe" => {
          val patternOpt = vector(1).asInstanceOf[Option[Array[Byte]]].map(UTF8StringReader.read)
          val patternsCount = vector(2).asInstanceOf[Int]
          PubSubMessage.PUnsubscribe(patternOpt, patternsCount)
        }
        case "message"      => {
          val channel = UTF8StringReader.read(vector(1).asInstanceOf[Option[Array[Byte]]].get)
          val message = vector(2).asInstanceOf[Option[Array[Byte]]].get
          PubSubMessage.Message(channel, message)
        }
        case "pmessage"     => {
          val pattern = UTF8StringReader.read(vector(1).asInstanceOf[Option[Array[Byte]]].get)
          val channel = UTF8StringReader.read(vector(2).asInstanceOf[Option[Array[Byte]]].get)
          val message = vector(3).asInstanceOf[Option[Array[Byte]]].get
          PubSubMessage.PMessage(pattern, channel, message)
        }
        case x              => throw RedisProtocolException(
          s"Invalid PubSubMessage type received: $x"
        )
      }
    }
    case x => throw RedisProtocolException(s"Invalid PubSubResponse received: $x")
  }
  
  private[scredis] def send[A](request: Request[A])(implicit targetActor: ActorRef): Future[A] = {
    aquire()
    targetActor ! request
    request.future
  }
  
  reporter.start()
  
}
