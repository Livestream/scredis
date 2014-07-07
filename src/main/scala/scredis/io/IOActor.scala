package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.codahale.metrics.MetricRegistry

import akka.actor._
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import akka.event.LoggingReceive

import scredis.protocol.{ Protocol, Request }
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.ServerRequests.Shutdown
import scredis.exceptions.RedisIOException

import scala.util.Failure
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import java.util.LinkedList
import java.net.InetSocketAddress

case object ClosingException extends Exception("Connection is being closed")

class IOActor(remote: InetSocketAddress) extends Actor with LazyLogging {
  
  import Tcp._
  import IOActor._
  import context.system
  import context.dispatcher
  
  import PartitionerActor._
  
  private val scheduler = context.system.scheduler
  
  /*
  private val requestMeter = scredis.protocol.NioProtocol.metrics.meter(
    MetricRegistry.name(getClass, "requestMeter")
  )
  
  private val bytesMeter = scredis.protocol.NioProtocol.metrics.meter(
    MetricRegistry.name(getClass, "bytesMeter")
  )
  
  private val writeTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "writeTimer")
  )
  private val waitTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "waitTimer")
  )
  private val tellTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "tellTimer")
  )*/
  
  private val bufferPool = new scredis.util.BufferPool(1)
  private val requests = new LinkedList[Request[_]]()
  private var batch: List[Request[_]] = Nil
  
  private var writeId: Int = 0
  private var canWrite = false
  private var connection: ActorRef = _
  private var partitionerActor: ActorRef = _
  private var waitTimerContext: com.codahale.metrics.Timer.Context = _
  
  private var retries: Int = 0
  private var isConnecting: Boolean = false
  private var timeoutCancellableOpt: Option[Cancellable] = None
  private var isClosing: Boolean = false
  
  private def incrementWriteId(): Unit = {
    if (writeId == Integer.MAX_VALUE) {
      writeId = 1
    } else {
      writeId += 1
    }
  }
  
  private def connect(): Unit = if (!isConnecting) {
    logger.info(s"Connecting to $remote")
    IO(Tcp) ! Connect(remote)
    isConnecting = true
  }
  
  private def close(): Unit = {
    connection ! Close
  }
  
  private def failAllQueuedRequests(throwable: Throwable): Unit = {
    requeueBatch()
    val count = requests.size
    while (!requests.isEmpty) {
      requests.pop().failure(throwable)
    }
    partitionerActor ! Remove(count)
  }
  
  private def failBatch(throwable: Throwable, skip: Boolean = false): Unit = {
    val count = batch.size
    batch.foreach(_.failure(throwable))
    batch = Nil
    if (skip) {
      partitionerActor ! Skip(count)
    } else {
      partitionerActor ! Remove(count)
    }
  }
  
  private def requeueBatch(): Unit = {
    batch.foreach(requests.push)
    batch = Nil
  }
  
  private def stop(): Unit = {
    logger.trace("Stopping Actor...")
    partitionerActor ! PoisonPill
    context.stop(self)
  }
  
  private def write(): Unit = {
    if (!this.batch.isEmpty) {
      requeueBatch()
    }
    if (requests.isEmpty) {
      canWrite = true
      return
    }
    
    //val ctx = writeTimer.time()
    var i = 0
    var length = 0
    val batch = ListBuffer[Request[_]]()
    while (!requests.isEmpty && i < 5000) {
      val request = requests.pop()
      request.encode()
      length += {
        request.encoded match {
          case Left(bytes) => bytes.length
          case Right(buffer) => buffer.remaining
        }
      }
      batch += request
      i += 1
    }
    val buffer = bufferPool.acquire(length)
    batch.foreach { r =>
      r.encoded match {
        case Left(bytes) => buffer.put(bytes)
        case Right(buff) => {
          buffer.put(buff)
          Protocol.releaseBuffer(buff)
        }
      }
    }
    buffer.flip()
    val data = ByteString(buffer)
    logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
    //requestMeter.mark(batch.size)
    //bytesMeter.mark(length)
    connection ! Write(data, WriteAck)
    bufferPool.release(buffer)
    canWrite = false
    incrementWriteId()
    this.batch = batch.toList
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(0 nanoseconds, self, WriteTimeout(writeId))
    }
    //ctx.stop()
    //waitTimerContext = waitTimer.time()
  }
  
  def receive: Receive = {
    case partitionerActor: ActorRef => {
      this.partitionerActor = partitionerActor
      logger.trace(s"Connecting to $remote...")
      connect()
      context.become(connecting)
    }
  }
  
  def connecting: Receive = LoggingReceive {
    case Connected(remote, local) => {
      logger.trace(s"Connected to $remote")
      connection = sender
      connection ! Register(self)
      isConnecting = false
      canWrite = true
      write()
      // TODO: context.watch(connection)
      if (isClosing) {
        context.become(closing)
      } else {
        context.become(connected)
      }
    }
    case CommandFailed(_: Connect) => {
      logger.error(s"Could not connect to $remote")
      failAllQueuedRequests(RedisIOException(s"Could not connect to $remote"))
      isConnecting = false
    }
    case request @ Quit() => {
      request.success(())
      partitionerActor ! Remove(1)
      failAllQueuedRequests(RedisIOException(s"Connection has been closed with QUIT command"))
      stop()
    }
    case request @ Shutdown(_) => {
      request.success(())
      partitionerActor ! Remove(1)
      failAllQueuedRequests(RedisIOException(s"Connection has been closed by SHUTDOWN command"))
      stop()
    }
    case request: Request[_] => {
      requests.addLast(request)
      if (!isConnecting) {
        connect()
      }
    }
  }
  
  def connected: Receive = LoggingReceive {
    case request: Request[_] => {
      //val data = ByteString(request.encoded)
      //logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
      requests.addLast(request)
      if (canWrite) {
        write()
      }
    }
    case request @ Quit() => {
      requests.addLast(request)
      if (canWrite) {
        write()
      }
      isClosing = true
      context.become(closing)
    }
    case request @ Shutdown(_) => {
      requests.addLast(request)
      if (canWrite) {
        write()
      }
      isClosing = true
      context.become(closing)
    }
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      //val ctx = tellTimer.time()
      partitionerActor ! data
      //ctx.stop()
    }
    case WriteAck => {
      //waitTimerContext.stop()
      retries = 0
      batch = Nil
      timeoutCancellableOpt.foreach(_.cancel())
      write()
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Timeout"))
      connection ! Abort
      context.become(aborting)
    }
    case CommandFailed(cmd @ Write(x, _)) => {
      logger.error(s"Command failed: $cmd")
      timeoutCancellableOpt.foreach(_.cancel())
      if (retries >= 5) {
        failBatch(RedisIOException("Could not send requests"))
        retries = 0
        write()
      } else {
        scheduler.scheduleOnce(retries * 1 seconds, connection, cmd)
        retries += 1
      }
    }
    case _: ConnectionClosed => {
      logger.info(s"Connection has been closed by the server")
      connect()
      context.become(connecting)
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been shutdown")
      connect()
      context.become(connecting)
    }
    case x => logger.error(s"Invalid message received from: $sender: $x")
  }
  
  def closing: Receive = LoggingReceive {
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      //val ctx = tellTimer.time()
      partitionerActor ! data
      //ctx.stop()
    }
    case WriteAck => {
      //waitTimerContext.stop()
      retries = 0
      batch = Nil
      timeoutCancellableOpt.foreach(_.cancel())
      write()
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Timeout"))
      connection ! Abort
      context.become(aborting)
    }
    case CommandFailed(cmd @ Write(x, _)) => {
      logger.error(s"Command failed: $cmd")
      timeoutCancellableOpt.foreach(_.cancel())
      if (retries >= 5) {
        failBatch(RedisIOException("Could not send request"))
        retries = 0
        write()
        if (canWrite) {
          connection ! Close
        }
      } else {
        scheduler.scheduleOnce(retries * 1 seconds, connection, cmd)
        retries += 1
      }
    }
    case Closed => {
      logger.info(s"Connection has been closed")
      stop()
    }
    case CommandFailed(c: CloseCommand) => {
      logger.warn(s"Connection could not be closed. Aborting...")
      connection ! Tcp.Abort
      stop()
    }
    case _: ConnectionClosed => {
      logger.info(s"Connection has been closed")
      stop()
    }
    case request: Request[_] => {
      request.failure(ClosingException)
      partitionerActor ! Remove(1)
    }
  }
  
  def aborting: Receive = LoggingReceive {
    case Aborted => {
      logger.info(s"ABORT: Connection has been closed by the server")
      connect()
      context.become(connecting)
    }
    case request: Request[_] => if (isClosing) {
      request.failure(ClosingException)
      partitionerActor ! Remove(1)
    } else {
      requests.addLast(request)
    }
    /*
    case _: ConnectionClosed => {
      logger.info(s"Connection has been closed by the server")
      connect()
      context.become(connecting)
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been shutdown")
      connect()
      context.become(connecting)
    }*/
  }
  
}

object IOActor {
  object WriteAck extends Tcp.Event
  case class WriteTimeout(writeId: Int)
}
