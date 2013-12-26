package scredis.nio

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.util.Logger
import scredis.protocol.{ NioProtocol, Request }

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }

import java.nio.ByteBuffer

class DecoderActor(ioActor: ActorRef) extends Actor {
  
  private val logger = Logger(getClass)
  private val requests = MQueue[Request[_]]()
  
  private var buffer: ByteBuffer = ByteBuffer.allocate(0)
  private var remainsOpt: Option[Array[Byte]] = None
  
  private var data = 0
  private var received = 0
  private var i = 0
  
  private def printBuffer(flip: Boolean): String = {
    if (flip) {
      buffer.flip()
    }
    val array = new Array[Byte](buffer.remaining)
    buffer.mark()
    buffer.get(array)
    buffer.reset()
    
    if (flip) {
      buffer.flip()
    }
    new String(array, "UTF-8").replace("\r", "\\r").replace("\n", "\\n")
  }
  
  private def concat(buffer: ByteBuffer): Unit = {
    val size = buffer.remaining + remainsOpt.map(_.length).getOrElse(0)
    if (size > this.buffer.remaining) {
      this.buffer = ByteBuffer.allocate(size)
    }
    remainsOpt.foreach { remains =>
      this.buffer.put(remains)
      remainsOpt = None
    }
    this.buffer.put(buffer)
    this.buffer.flip()
    this.buffer.rewind()
  }
  
  private def clear(): Unit = {
    if (buffer.remaining > 0) {
      val array = new Array[Byte](buffer.remaining)
      buffer.get(array)
      remainsOpt = Some(array)
    }
    buffer.clear()
  }
  
  def receive: Receive = {
    case request: Request[_] => {
      assert(request.encoded != null)
      requests.enqueue(request)
      ioActor ! request
    }
    case data: ByteString => {
      logger.trace(s"Decoding ${requests.size} requests")
      
      concat(data.asByteBuffer)
      
      var hasNext = (buffer.remaining > 0)
      while (hasNext) {
        buffer.mark()
        val reply = try {
          val reply = NioProtocol.decode(buffer)
          requests.dequeue().complete(Success(reply))
          i += 1
          if (i % 100000 == 0) logger.info(i.toString)
          hasNext = (buffer.remaining > 0)
        } catch {
          case e: java.nio.BufferUnderflowException => {
            buffer.reset()
            hasNext = false
          }
        }
      }
      clear()
    }
  }
  
}
