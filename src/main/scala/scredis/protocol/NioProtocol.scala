package scredis.protocol

import akka.actor.ActorRef

import scredis.util.BufferPool

import scala.concurrent.{ Future, Promise }

import java.nio.ByteBuffer

object NioProtocol {
  
  private val Encoding = "UTF-8"

  private val StatusReply = '+'.toByte
  private val ErrorReply = '-'.toByte
  private val IntegerReply = ':'.toByte
  private val BulkReply = '$'.toByte
  private val BulkReplyLength = 1
  private val MultiBulkReply = '*'.toByte
  private val MultiBulkReplyLength = 1
  
  private val CrLf = "\r\n".getBytes(Encoding)
  private val CrLfLength = CrLf.length
  
  private val bufferPool = new BufferPool(256)
  
  def multiBulkRequestFor(command: String): ByteBuffer = multiBulkRequestFor(command, Nil)
  
  def multiBulkRequestFor(command: String, args: Seq[Any]): ByteBuffer = {
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
    
    buffer.put(MultiBulkReply).put(argsSize).put(CrLf)
    for ((serializedArg, serializedArgSize) <- serializedArgs) {
      buffer.put(BulkReply).put(serializedArgSize).put(CrLf).put(serializedArg).put(CrLf)
    }
    
    buffer.flip()
    buffer
  }
  
  def send[A](command: Command[A])(implicit ioActor: ActorRef): Future[A] = {
    ioActor ! command
    command.future
  }
  
}