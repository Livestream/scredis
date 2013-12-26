package scredis.nio

import akka.io.{ IO, Tcp }
import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.util.Logger
import scredis.protocol.Request

import scala.util.Failure
import scala.collection.mutable.{ Queue => MQueue, ListBuffer }

import java.net.InetSocketAddress

case class Batch(requests: Seq[Request[_]], length: Int)
case object ClosingException extends Exception("Connection is being closed")

class IOActor(remote: InetSocketAddress) extends Actor {
 
  import Tcp._
  import context.system
  
  object WriteAck extends Event
  
  private val bufferPool = new scredis.util.BufferPool(256)
  private val logger = Logger(getClass)
  private val requests = MQueue[Request[_]]()
  
  private var canWrite = false
  var written = 0
  private var connection: ActorRef = _
  private var decoderActor: ActorRef = _
  
  private def stop(): Unit = {
    logger.trace("Stopping Actor...")
    context.stop(self)
  }
  
  private def write(): Unit = {
    if (requests.isEmpty) {
      canWrite = true
      return
    }
    
    var i = 0
    var length = 0
    val batch = ListBuffer[Request[_]]()
    while (!requests.isEmpty && i < 10000) {
      val request = requests.dequeue()
      length += request.encoded.remaining
      batch += request
      i += 1
    }
    //println(batch.size)
    val buffer = bufferPool.acquire(length)
    batch.foreach { r =>
      buffer.put(r.encoded)
    }
    buffer.flip()
    val data = ByteString(buffer)
    logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
    connection ! Write(data, WriteAck)
    bufferPool.release(buffer)
    batch.foreach(x => scredis.protocol.NioProtocol.bufferPool.release(x.encoded))
    canWrite = false
  }
  
  def receive: Receive = {
    case decoderActor: ActorRef => {
      this.decoderActor = decoderActor
      logger.trace(s"Connecting to $remote...")
      IO(Tcp) ! Connect(remote)
      context.become(connecting)
    }
  }
  
  def connecting: Receive = {
    case c @ Connected(remote, local) => {
      logger.trace(s"Connected to $remote")
      connection = sender
      connection ! Register(self)
      if (!requests.isEmpty) {
        self ! requests.toList
        requests.clear()
      }
      canWrite = true
      context.become(ready)
    }
    case CommandFailed(_: Connect) => {
      logger.error(s"Could not connect to $remote")
      stop()
    }
    case request: Request[_] => requests += request
    case requests: Seq[Request[_] @unchecked] => this.requests ++= requests
  }
  
  import context.dispatcher
  import scala.concurrent.duration._
  //context.system.scheduler.schedule(100 millisecond, 100 millisecond, self, 'Write)
  
  def ready: Receive = {
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      decoderActor ! data
    }
    case WriteAck => write()
    case request: Request[_] => {
      //val data = ByteString(request.encoded)
      //logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
      requests.enqueue(request)
      if (canWrite) {
        write()
      }
    }
    case CommandFailed(w: Write) => // O/S buffer was full
    case Close => {
      logger.trace(s"Closing connection...")
      connection ! Close
      context.become(closing)
    }
    case _: ConnectionClosed => {
      logger.debug(s"Connection has been closed by the server")
      stop()
    }
    case x => logger.error(x.toString)
  }
  
  def closing: Receive = {
    case CommandFailed(c: CloseCommand) => {
      logger.warn(s"Connection could not be closed. Aborting...")
      connection ! Tcp.Abort
      stop()
    }
    case _: ConnectionClosed => {
      logger.debug(s"Connection has been closed")
      stop()
    }
    case request: Request[_] => {
      request.complete(Failure(ClosingException))
    }
    case requests: Seq[Request[_] @unchecked] => {
      requests.foreach(_.complete(Failure(ClosingException)))
    }
  }
  
}
