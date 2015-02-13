package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor._
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import akka.event.LoggingReceive

import scredis.protocol.{ Protocol, Request }
import scredis.exceptions.RedisIOException

import scala.util.{ Success, Failure }
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._

import java.util.LinkedList
import java.net.InetSocketAddress

class IOActor(
  listenerActor: ActorRef,
  remote: InetSocketAddress,
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int
) extends Actor with LazyLogging {
  
  import Tcp._
  import IOActor._
  import context.system
  import context.dispatcher
  
  private val scheduler = context.system.scheduler
  
  private val bufferPool = new scredis.util.BufferPool(
    1,
    maxWriteBatchSize + (0.25 * maxWriteBatchSize).toInt
  )
  
  private var batch: Seq[Request[_]] = Nil
  private var canWrite = false
  private var timeoutCancellableOpt: Option[Cancellable] = None
  
  protected val requests = new LinkedList[Request[_]]()
  protected var connection: ActorRef = _
  
  protected def connect(): Unit = {
    logger.info(s"Connecting to $remote")
    IO(Tcp) ! Connect(
      remoteAddress = remote,
      options = List[akka.io.Inet.SocketOption](
        SO.KeepAlive(true),
        SO.TcpNoDelay(true),
        SO.ReuseAddress(true),
        SO.SendBufferSize(tcpSendBufferSizeHint),
        SO.ReceiveBufferSize(tcpReceiveBufferSizeHint)
      ),
      timeout = Some(connectTimeout)
    )
    timeoutCancellableOpt = Some {
      scheduler.scheduleOnce(2 seconds, self, ConnectTimeout)
    }
  }
  
  protected def requeueBatch(): Unit = {
    batch.foreach(requests.push)
    batch = Nil
  }
  
  protected def abort(): Unit = {
    listenerActor ! ListenerActor.Abort
    become(awaitingAbort)
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
    requests.foreach { request =>
      request.encoded match {
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
    this.batch = requests
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
    while (!requests.isEmpty && length < maxWriteBatchSize) {
      val request = requests.pop()
      length += encode(request)
      batch += request
    }
    write(batch.toList, Some(length))
  }
  
  protected def always: Receive = {
    case Shutdown => abort()
    case Terminated(_) => {
      listenerActor ! ListenerActor.Shutdown
      become(awaitingShutdown)
    }
  }
  
  protected def fail: Receive = {
    case x => logger.error(s"Received unhandled message: $x")
  }
  
  protected def become(state: Receive): Unit = context.become(state orElse always orElse fail)
  
  override def preStart(): Unit = {
    connect()
    become(connecting)
  }
  
  def receive: Receive = fail
  
  def connecting: Receive = {
    case Connected(remote, local) => {
      logger.info(s"Connected to $remote")
      connection = sender
      connection ! Register(listenerActor)
      context.watch(connection)
      listenerActor ! ListenerActor.Connected
      timeoutCancellableOpt.foreach(_.cancel())
      canWrite = true
      requeueBatch()
      write()
      become(connected)
    }
    case CommandFailed(_: Connect) => {
      logger.error(s"Could not connect to $remote: Command failed")
      timeoutCancellableOpt.foreach(_.cancel())
      context.stop(self)
    }
    case ConnectTimeout => {
      logger.error(s"Could not connect to $remote: Connect timeout")
      context.stop(self)
    }
  }
  
  def connected: Receive = {
    case request: Request[_] => {
      requests.addLast(request)
      if (canWrite) {
        write()
      }
    }
    case WriteAck => {
      batch = Nil
      write()
    }
    case CommandFailed(_: Write) => {
      logger.error(s"Write failed")
      write()
    }
  }
  
  def awaitingShutdown: Receive = {
    case request: Request[_] => {
      request.failure(RedisIOException("Connection is beeing shutdown"))
      listenerActor ! ListenerActor.Remove(1)
    }
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case ShutdownAck => context.stop(self)
  }
  
  def awaitingAbort: Receive = {
    case request: Request[_] => {
      request.failure(RedisIOException("Connection is beeing reset"))
      listenerActor ! ListenerActor.Remove(1)
    }
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case AbortAck => {
      connection ! Abort
      timeoutCancellableOpt = Some {
        scheduler.scheduleOnce(3 seconds, self, AbortTimeout)
      }
      become(aborting)
    }
  }
  
  def aborting: Receive = {
    case WriteAck =>
    case CommandFailed(_: Write) =>
    case Aborted =>
    case Terminated(_) => {
      timeoutCancellableOpt.foreach(_.cancel())
      context.stop(self)
    }
    case AbortTimeout => {
      logger.error(s"A timeout occurred while resetting the connection")
      context.stop(connection)
    }
  }
  
}

object IOActor {
  object WriteAck extends Tcp.Event
  case object ConnectTimeout
  case object AbortTimeout
  case object AbortAck
  case object ShutdownAck
  case object Shutdown
}
