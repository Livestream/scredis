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
import scala.concurrent.{ Future, Await }
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
  
  private var batch: Seq[Request[_]] = Nil
  private var writeId = 0
  private var retries = 0
  private var canWrite = false
  private var isConnecting: Boolean = false
  private var isAuthenticating = false
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
    IO(Tcp) ! Connect(
      remoteAddress = remote,
      options = List[akka.io.Inet.SocketOption](
        SO.KeepAlive(true),
        SO.TcpNoDelay(true),
        SO.ReuseAddress(true),
        SO.SendBufferSize(5000000),
        SO.ReceiveBufferSize(500000)
      ),
      timeout = Some(2 seconds)
    )
    isConnecting = true
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(2 seconds, self, ConnectTimeout)
    }
  }
  
  protected def failAllQueuedRequests(throwable: Throwable): Unit = {
    requeueBatch()
    val count = requests.size
    while (!requests.isEmpty) {
      requests.pop().failure(throwable)
    }
    partitionerActor ! Remove(count)
  }
  
  protected def failBatch(throwable: Throwable): Unit = {
    val count = batch.size
    batch.foreach(_.failure(throwable))
    batch = Nil
    partitionerActor ! Remove(count)
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
    become(aborting)
  }
  
  protected def stop(): Unit = {
    logger.trace("Stopping Actor...")
    partitionerActor ! PoisonPill
    context.stop(self)
  }
  
  protected def encode(request: Request[_]): Int = {
    request.encode()
    request.encoded match {
      case Left(bytes) => bytes.length
      case Right(buffer) => buffer.remaining
    }
  }
  
  protected def write(requests: Seq[Request[_]], lengthOpt: Option[Int] = None): Unit = {
    val length = lengthOpt.getOrElse {
      requests.foldLeft(0)((length, request) => length + encode(request))
    }
    val buffer = bufferPool.acquire(length)
    requests.foreach { r =>
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
    this.batch = requests
    /*
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(2 seconds, self, WriteTimeout(writeId))
    }*/
  }
  
  protected def write(): Unit = {
    if (!this.batch.isEmpty) {
      requeueBatch()
    }
    if (requests.isEmpty) {
      canWrite = true
      return
    }
    
    var length = 0
    val batch = ListBuffer[Request[_]]()
    while (!requests.isEmpty && length < 50000) {
      val request = requests.pop()
      length += encode(request)
      batch += request
    }
    write(batch.toList, Some(length))
  }
  
  protected def onConnect(): Unit = {
    val authRequestOpt = passwordOpt.map { password =>
      Auth(password)
    }
    val selectRequestOpt = if (database > 0) {
      Some(Select(database))
    } else {
      None
    }
    
    val authFuture = authRequestOpt match {
      case Some(request) => request.future
      case None => Future.successful(())
    }
    val selectFuture = selectRequestOpt match {
      case Some(request) => request.future
      case None => Future.successful(())
    }
    
    (authRequestOpt, selectRequestOpt) match {
      case (Some(auth), Some(select)) => {
        partitionerActor ! PartitionerActor.Push(select)
        partitionerActor ! PartitionerActor.Push(auth)
        write(Seq(auth, select))
      }
      case (Some(auth), None) => {
        partitionerActor ! PartitionerActor.Push(auth)
        write(Seq(auth))
      }
      case (None, Some(select)) => {
        partitionerActor ! PartitionerActor.Push(select)
        write(Seq(select))
      }
      case (None, None) =>
    }
    
    authFuture.andThen {
      case Success(_) => selectFuture.andThen {
        case Success(_) =>
        case Failure(e) => logger.error(s"Could not select database '$database' in $remote", e)
      }
      case Failure(e) => logger.error(s"Could not authenticate to $remote", e)
    }
  }
  
  protected def onAuthAndSelect(): Unit = ()
  
  protected def all: Receive = PartialFunction.empty
  
  protected def fail: Receive = {
    case x => logger.error(s"Received unhandled message: $x")
  }
  
  protected def become(state: Receive): Unit = context.become(all orElse state orElse fail)
  
  override def preStart(): Unit = {
    become(receive)
  }
  
  def receive: Receive = {
    case partitionerActor: ActorRef => {
      this.partitionerActor = partitionerActor
      logger.trace(s"Connecting to $remote...")
      connect()
      become(connecting)
    }
  }
  
  def connecting: Receive = {
    case Connected(remote, local) => {
      logger.info(s"Connected to $remote")
      connection = sender
      connection ! Register(self)
      context.watch(connection)
      timeoutCancellableOpt.foreach(_.cancel())
      isConnecting = false
      canWrite = true
      retries = 0
      requeueBatch()
      onConnect()
      if (canWrite) {
        write()
      } else {
        isAuthenticating = true
      }
      if (isClosing) {
        become(closing)
      } else {
        become(connected)
      }
    }
    case CommandFailed(_: Connect) => {
      logger.error(s"Could not connect to $remote: Command failed")
      failAllQueuedRequests(RedisIOException(s"Could not connect to $remote: Command failed"))
      timeoutCancellableOpt.foreach(_.cancel())
      isConnecting = false
    }
    case ConnectTimeout => {
      logger.error(s"Could not connect to $remote: Connect timeout")
      failAllQueuedRequests(RedisIOException(s"Could not connect to $remote: Connect timeout"))
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
  
  def connected: Receive = {
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
          become(closing)
        }
        case _  => requests.addLast(request)
      }
      if (!isAuthenticating && canWrite) {
        write()
      }
    }
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      partitionerActor ! data
      if (isAuthenticating) {
        onAuthAndSelect()
        isAuthenticating = false
        if (canWrite) {
          write()
        }
      }
    }
    case WriteAck => {
      batch = Nil
      retries = 0
      timeoutCancellableOpt.foreach(_.cancel())
      if (isAuthenticating) {
        canWrite = true
      } else {
        write()
      }
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Write timeout"))
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
        if (isAuthenticating) {
          canWrite = true
        } else {
          write()
        }
      }
    }
    case _: ConnectionClosed => {
      logger.info(s"Connection has been closed by the server")
      connect()
      become(connecting)
    }
    case Terminated(connection) => {
      logger.info(s"Connection has been shutdown")
      connect()
      become(connecting)
    }
    case PoisonPill => {
      logger.info(s"Closing connection")
      connection ! Close
      become(closing)
    }
  }
  
  def closing: Receive = {
    case request: Request[_] => {
      request.failure(RedisIOException("Connection is being closed"))
      partitionerActor ! Remove(1)
    }
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      partitionerActor ! data
      if (isAuthenticating) {
        onAuthAndSelect()
        isAuthenticating = false
        if (canWrite) {
          write()
        }
      }
    }
    case WriteAck => {
      retries = 0
      batch = Nil
      timeoutCancellableOpt.foreach(_.cancel())
      if (isAuthenticating) {
        canWrite = true
      } else {
        write()
      }
    }
    case WriteTimeout(writeId) => if (writeId == this.writeId) {
      failBatch(RedisIOException("Write timeout"))
      abortAndReconnect()
    }
    case CommandFailed(cmd: Write) => {
      logger.error(s"Command failed: $cmd")
      timeoutCancellableOpt.foreach(_.cancel())
      if (retries >= 2) {
        failBatch(RedisIOException("Could not send requests"))
        abortAndReconnect()
      } else {
        retries += 1
        if (isAuthenticating) {
          canWrite = true
        } else {
          write()
        }
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
  
  def aborting: Receive = {
    case Received(data) => println("RECEIVED")
    case WriteAck => println("WRITEACK")
    case CommandFailed(cmd: Write) => println("COMMANDFAILED")
    case _: ConnectionClosed =>
    case Terminated(connection) => {
      logger.info(s"Connection has been reset")
      timeoutCancellableOpt.foreach(_.cancel())
      connect()
      become(connecting)
    }
    case AbortTimeout => {
      logger.error(s"A timeout occurred while resetting the connection")
      context.stop(connection)
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
  case object ConnectTimeout
  case class WriteTimeout(writeId: Int)
  case object AbortTimeout
}
