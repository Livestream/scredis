package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.codahale.metrics.MetricRegistry

import akka.actor._
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import akka.event.LoggingReceive

import scredis.protocol.{ Protocol, Request }
import scredis.protocol.requests.ConnectionRequests.{ Auth, Select, Quit }
import scredis.protocol.requests.ServerRequests.Shutdown
import scredis.protocol.requests.PubSubRequests.{ Subscribe => SubscribeRequest, PSubscribe }
import scredis.exceptions.RedisIOException

import scala.util.{ Success, Failure }
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._

import java.util.LinkedList
import java.net.InetSocketAddress

class IOActor(
  remote: InetSocketAddress, var passwordOpt: Option[String], var database: Int
) extends Actor with LazyLogging {
  
  import Tcp._
  import IOActor._
  import context.system
  import context.dispatcher
  
  import PartitionerActor._
  
  private val scheduler = context.system.scheduler
  
  private val bufferPool = new scredis.util.BufferPool(1)
  
  private var batch: List[Request[_]] = Nil
  private var writeId = 0
  private var retries = 0
  private var canWrite = false
  private var isConnecting: Boolean = false
  private var isClosing: Boolean = false
  private var timeoutCancellableOpt: Option[Cancellable] = None
  
  protected val requests = new LinkedList[Request[_]]()
  protected var connection: ActorRef = _
  protected var partitionerActor: ActorRef = _
  
  protected def incrementWriteId(): Unit = {
    if (writeId == Integer.MAX_VALUE) {
      writeId = 1
    } else {
      writeId += 1
    }
  }
  
  protected def connect(): Unit = if (!isConnecting) {
    logger.info(s"Connecting to $remote")
    IO(Tcp) ! Connect(remote)
    isConnecting = true
  }
  
  protected def failAllQueuedRequests(throwable: Throwable): Unit = {
    requeueBatch()
    val count = requests.size
    while (!requests.isEmpty) {
      requests.pop().failure(throwable)
    }
    partitionerActor ! Remove(count)
  }
  
  protected def failBatch(throwable: Throwable, skip: Boolean = false): Unit = {
    val count = batch.size
    batch.foreach(_.failure(throwable))
    batch = Nil
    if (skip) {
      partitionerActor ! Skip(count)
    } else {
      partitionerActor ! Remove(count)
    }
  }
  
  protected def requeueBatch(): Unit = {
    batch.foreach(requests.push)
    batch = Nil
  }
  
  protected def abortAndReconnect(): Unit = {
    connection ! Abort
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(3 seconds, self, AbortTimeout)
    }
    context.become(aborting)
  }
  
  protected def stop(): Unit = {
    logger.trace("Stopping Actor...")
    partitionerActor ! PoisonPill
    context.stop(self)
  }
  
  protected def write(): Unit = {
    if (!this.batch.isEmpty) {
      requeueBatch()
    }
    if (requests.isEmpty) {
      canWrite = true
      return
    }
    
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
    connection ! Write(data, WriteAck)
    bufferPool.release(buffer)
    canWrite = false
    incrementWriteId()
    this.batch = batch.toList
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(5 seconds, self, WriteTimeout(writeId))
    }
  }
  
  protected def onConnect(): Unit = {
    val selectFuture = if (database > 0) {
      val request = Select(database)
      requests.push(request)
      partitionerActor ! Push(request)
      request.future
    } else {
      Future.successful(())
    }
    val passwordFuture = passwordOpt match {
      case Some(password) => {
        val request = Auth(password)
        requests.push(request)
        partitionerActor ! Push(request)
        request.future
      }
      case None => Future.successful(())
    }
    passwordFuture.andThen {
      case Success(_) => selectFuture.andThen {
        case Success(_) => onAuthAndSelect()
        case Failure(e) => logger.error(s"Could not select database '$database'' in $remote", e)
      }
      case Failure(e) => logger.error(s"Could not authenticate to $remote", e)
    }
  }
  
  protected def onAuthAndSelect(): Unit = ()
  
  protected def all: Receive = PartialFunction.empty
  
  def receive: Receive = {
    case partitionerActor: ActorRef => {
      this.partitionerActor = partitionerActor
      logger.trace(s"Connecting to $remote...")
      connect()
      context.become(connecting)
    }
  }
  
  def connecting: Receive = all orElse {
    case Connected(remote, local) => {
      logger.trace(s"Connected to $remote")
      connection = sender
      connection ! Register(self)
      isConnecting = false
      canWrite = true
      retries = 0
      requeueBatch()
      onConnect()
      write()
      context.watch(connection)
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
      failAllQueuedRequests(RedisIOException(s"Connection has been closed with SHUTDOWN command"))
      stop()
    }
    case request: Request[_] => {
      request match {
        case Auth(password) => if (password.isEmpty) {
          passwordOpt = None
          request.success(())
          partitionerActor ! Remove(1)
        } else {
          passwordOpt = Some(password)
          requests.addLast(request)
        }
        case Select(database) => {
          this.database = database
          requests.addLast(request)
        }
        case _  => requests.addLast(request)
      }
      if (!isConnecting) {
        connect()
      }
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been shutdown")
    }
    case PoisonPill => stop()
  }
  
  def connected: Receive = all orElse {
    case request: Request[_] => {
      request match {
        case Auth(password) => if (password.isEmpty) {
          passwordOpt = None
          request.success(())
          partitionerActor ! Remove(1)
        } else {
          passwordOpt = Some(password)
          requests.addLast(request)
        }
        case Select(database) => {
          this.database = database
          requests.addLast(request)
        }
        case Quit() | Shutdown(_) => {
          isClosing = true
          requests.addLast(request)
          context.become(closing)
        }
        case _  => requests.addLast(request)
      }
      if (canWrite) {
        write()
      }
    }
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      partitionerActor ! data
    }
    case WriteAck => {
      batch = Nil
      retries = 0
      timeoutCancellableOpt.foreach(_.cancel())
      write()
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Timeout"))
      abortAndReconnect()
    }
    case CommandFailed(cmd @ Write(x, _)) => {
      logger.error(s"Command failed: $cmd")
      timeoutCancellableOpt.foreach(_.cancel())
      if (retries >= 2) {
        failBatch(RedisIOException("Could not send requests"))
        abortAndReconnect()
      } else {
        retries += 1
        write()
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
    case PoisonPill => {
      logger.info(s"Closing connection")
      connection ! Close
      context.become(closing)
    }
    case x => logger.error(s"Invalid message received from: $sender: $x")
  }
  
  def closing: Receive = all orElse {
    case request: Request[_] => {
      request.failure(RedisIOException("Connection is being closed"))
      partitionerActor ! Remove(1)
    }
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      partitionerActor ! data
    }
    case WriteAck => {
      retries = 0
      batch = Nil
      timeoutCancellableOpt.foreach(_.cancel())
      write()
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Timeout"))
      abortAndReconnect()
    }
    case CommandFailed(cmd @ Write(x, _)) => {
      logger.error(s"Command failed: $cmd")
      timeoutCancellableOpt.foreach(_.cancel())
      if (retries >= 2) {
        failBatch(RedisIOException("Could not send requests"))
        abortAndReconnect()
      } else {
        retries += 1
        write()
      }
    }
    case _: ConnectionClosed => {
      logger.info(s"Connection has been closed")
      stop()
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been shutdown")
      stop()
    }
    case PoisonPill =>
  }
  
  def aborting: Receive = all orElse {
    case _: ConnectionClosed => {
      logger.info(s"Connection has been reset")
      connect()
      context.become(connecting)
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been reset")
      connect()
      context.become(connecting)
    }
    case AbortTimeout => {
      logger.error(s"A timeout occurred while resetting the connection")
      connect()
      context.become(connecting)
    }
    case request: Request[_] => if (isClosing) {
      request.failure(RedisIOException("Connection is closing"))
      partitionerActor ! Remove(1)
    } else {
      request match {
        case Auth(password) => if (password.isEmpty) {
          passwordOpt = None
          request.success(())
          partitionerActor ! Remove(1)
        } else {
          passwordOpt = Some(password)
          requests.addLast(request)
        }
        case Select(database) => {
          this.database = database
          requests.addLast(request)
        }
        case Quit() | Shutdown(_) => {
          isClosing = true
          requests.addLast(request)
        }
        case _ => requests.addLast(request)
      }
    }
    case PoisonPill => isClosing = true
  }
  
}

object IOActor {
  object WriteAck extends Tcp.Event
  case class WriteTimeout(writeId: Int)
  case object AbortTimeout
}
