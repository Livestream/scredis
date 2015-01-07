package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor._
import akka.routing._
import akka.io.Tcp
import akka.util.ByteString

import scredis.{ Transaction, PubSubMessage }
import scredis.protocol.{ Protocol, Request }
import scredis.protocol.requests.ConnectionRequests.{ Auth, Select, Quit }
import scredis.protocol.requests.ServerRequests
import scredis.protocol.requests.TransactionRequests.{ Multi, Exec }
import scredis.exceptions.RedisIOException
import scredis.util.UniqueNameGenerator

import scala.util.{ Try, Success, Failure }
import scala.collection.mutable.{ Queue => MQueue, ListBuffer }
import scala.concurrent.{ ExecutionContext, Promise, Future }
import scala.concurrent.duration._

import java.util.LinkedList
import java.nio.ByteBuffer
import java.net.InetSocketAddress

class ListenerActor(
  host: String,
  port: Int,
  var passwordOpt: Option[String],
  var database: Int,
  var nameOpt: Option[String],
  var decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String
) extends Actor with LazyLogging {
  
  import context.dispatcher
  
  import ListenerActor._
  
  override val supervisorStrategy = OneForOneStrategy() {
    case e: Exception => SupervisorStrategy.Stop
  }
  
  private val remote = new InetSocketAddress(host, port)
  
  private var remainingByteStringOpt: Option[ByteString] = None
  private var initializationRequestsCount = 0
  private var isConnecting = false
  private var isShuttingDown = false
  private var isShuttingDownBeforeConnected = false
  private var isReceiveTimeout = false
  private var timeoutCancellableOpt: Option[Cancellable] = None
  
  protected val queuedRequests = new LinkedList[Request[_]]()
  protected val requests = new LinkedList[Request[_]]()
  protected var ioActor: ActorRef = _
  protected var decoders: Router = _
  
  private def createIOActor(): ActorRef = context.actorOf(
    Props(
      classOf[IOActor],
      self,
      remote,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint
    ).withDispatcher(akkaIODispatcherPath),
    UniqueNameGenerator.getUniqueName(s"${nameOpt.getOrElse(s"$host-$port")}-io-actor")
  )
  
  private def createDecodersRouter(): Router = {
    val routees = Vector.fill(decodersCount) {
      val ref = context.actorOf(
        Props(classOf[DecoderActor]).withDispatcher(akkaIODispatcherPath),
        UniqueNameGenerator.getNumberedName(s"${nameOpt.getOrElse(s"$host-$port")}-decoder-actor")
      )
      context.watch(ref)
      ActorRefRoutee(ref)
    }
    Router(SmallestMailboxRoutingLogic(), routees)
  }
  
  protected def doSend(request: Request[_]): Unit = {
    receiveTimeoutOpt.foreach { receiveTimeout =>
      if (timeoutCancellableOpt.isEmpty) {
        timeoutCancellableOpt = Some {
          context.system.scheduler.scheduleOnce(receiveTimeout, self, ReceiveTimeout)
        }
      }
    }
    requests.addLast(request)
    ioActor ! request
  }
  
  protected def send(requests: Request[_]*): Unit = {
    var isShuttingDown = false
    requests.foreach { request =>
      if (isShuttingDown) {
        request.failure(RedisIOException("Shutting down"))
      } else {
        request match {
          case auth @ Auth(password) => if (password.isEmpty) {
            passwordOpt = None
            auth.success(())
          } else {
            passwordOpt = Some(password)
            doSend(auth)
          }
          case select @ Select(database) => {
            this.database = database
            doSend(select)
          }
          case setName @ ServerRequests.ClientSetName(name) => if (name.isEmpty) {
            nameOpt = None
            doSend(setName)
          } else {
            nameOpt = Some(name)
            doSend(setName)
          }
          case request @ (Quit() | ServerRequests.Shutdown(_)) => {
            isShuttingDown = true
            this.isShuttingDown = true
            doSend(request)
            become(shuttingDown)
          }
          case _ => doSend(request)
        }
      }
    }
  }
  
  protected def sendAllQueuedRequests(): Unit = while (!queuedRequests.isEmpty) {
    send(queuedRequests.pop())
  }
  
  protected def failAllQueuedRequests(throwable: Throwable): Unit = {
    while (!queuedRequests.isEmpty) {
      queuedRequests.pop().failure(throwable)
    }
  }
  
  protected def failAllSentRequests(throwable: Throwable): Unit = {
    while (!requests.isEmpty) {
      requests.pop().failure(throwable)
    }
  }
  
  protected def handleData(data: ByteString, responsesCount: Int): Unit = {
    val requestsCount = this.requests.size
    val (count, skip) = if (responsesCount > requestsCount) {
      (requestsCount, responsesCount - requestsCount)
    } else {
      (responsesCount, 0)
    }
    val requests = ListBuffer[Request[_]]()
    for (i <- 1 to count) {
      requests += this.requests.pop()
    }
    decoders.route(DecoderActor.Partition(data, requests.toList.iterator, skip), self)
  }
  
  protected def receive(data: ByteString): Int = {
    logger.debug(s"Received data: ${data.decodeString("UTF-8").replace("\r\n", "\\r\\n")}")
    
    timeoutCancellableOpt.foreach(_.cancel())
    timeoutCancellableOpt = None
    
    val completedData = remainingByteStringOpt match {
      case Some(remains) => remains ++ data
      case None => data
    }
    
    val buffer = completedData.asByteBuffer
    val responsesCount = Protocol.count(buffer)
    val position = buffer.position
    
    if (buffer.remaining > 0) {
      remainingByteStringOpt = Some(ByteString(buffer))
    } else {
      remainingByteStringOpt = None
    }
    
    if (responsesCount > 0) {
      val trimmedData = if (remainingByteStringOpt.isDefined) {
        completedData.take(position)
      } else {
        completedData
      }
      handleData(trimmedData, responsesCount)
    }
    responsesCount
  }
  
  protected def unhandled: Receive = {
    case x => logger.error(s"Received unexpected message: $x")
  }
  
  protected def always: Receive = {
    case Remove(count) => for (i <- 1 to count) {
      requests.pop()
    }
    case Abort => {
      ioActor ! IOActor.AbortAck
      become(reconnecting)
    }
    case Shutdown => {
      ioActor ! IOActor.ShutdownAck
      become(reconnecting)
    }
    case _: Tcp.ConnectionClosed =>
  }
  
  protected def fail: Receive = {
    case request: Request[_] => request.failure(RedisIOException("Shutting down"))
    case transaction: Transaction => transaction.execRequest.failure(
      RedisIOException("Shutting down")
    )
  }
  
  protected def queue: Receive = {
    case request: Request[_] => queuedRequests.addLast(request)
    case t @ Transaction(requests) => {
      queuedRequests.addLast(t.multiRequest)
      requests.foreach { request =>
        queuedRequests.addLast(request)
      }
      queuedRequests.addLast(t.execRequest)
    }
  }
  
  protected def send: Receive = {
    case request: Request[_] => send(request)
    case t @ Transaction(requests) => {
      send(t.multiRequest)
      send(requests: _*)
      send(t.execRequest)
    }
  }
  
  protected def become(state: Receive): Unit = context.become(state orElse always orElse unhandled)
  
  protected def reconnect(): Unit = {
    ioActor = createIOActor()
    context.watch(ioActor)
    isConnecting = true
    isReceiveTimeout = false
    become(connecting)
  }
  
  protected def handleReceiveTimeout(): Unit = {
    logger.error("Receive timeout")
    isReceiveTimeout = true
    timeoutCancellableOpt = None
    ioActor ! IOActor.Shutdown
    become(reconnecting)
  }
  
  protected def onConnect(): Unit = ()
  protected def onInitialized(): Unit = ()
  
  protected def shutdown(): Unit = {
    decoders.route(Broadcast(PoisonPill), self)
    become(awaitingDecodersShutdown)
  }
  
  override def preStart(): Unit = {
    ioActor = createIOActor()
    decoders = createDecodersRouter()
    context.watch(ioActor)
    isConnecting = true
    become(connecting)
  }
  
  def receive: Receive = unhandled
  
  def connecting: Receive = {
    case request: Quit => {
      request.success(())
      failAllQueuedRequests(RedisIOException("Connection has been shutdown by QUIT command"))
      isShuttingDownBeforeConnected = true
    }
    case request: Request[_] => {
      queuedRequests.addLast(request)
      if (!isConnecting) {
        reconnect()
      }
    }
    case t @ Transaction(requests) => {
      queuedRequests.addLast(t.multiRequest)
      requests.foreach { request =>
        queuedRequests.addLast(request)
      }
      queuedRequests.addLast(t.execRequest)
      if (!isConnecting) {
        reconnect()
      }
    }
    case Connected => {
      isConnecting = false
      
      if (isShuttingDownBeforeConnected) {
        shutdown()
      } else {
        onConnect()
        
        val authRequestOpt = passwordOpt.map { password =>
          Auth(password)
        }
        val selectRequestOpt = if (database > 0) {
          Some(Select(database))
        } else {
          None
        }
        val setNameRequestOpt = nameOpt.map { name =>
          ServerRequests.ClientSetName(name)
        }
        
        val authFuture = authRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }
        val selectFuture = selectRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }
        val setNameFuture = setNameRequestOpt match {
          case Some(request) => request.future
          case None => Future.successful(())
        }
        
        val requests = List[Option[Request[Unit]]](
          authRequestOpt, selectRequestOpt, setNameRequestOpt
        ).flatten
        
        initializationRequestsCount = requests.size
        
        if (initializationRequestsCount > 0) {
          send(requests: _*)
          become(initializing)
        } else {
          onInitialized()
          sendAllQueuedRequests()
          if (isShuttingDown) {
            become(shuttingDown)
          } else {
            become(initialized)
          }
        }
        
        authFuture.recover {
          case e: Throwable => logger.error(s"Could not authenticate to $remote", e)
        }
        selectFuture.recover {
          case e: Throwable => logger.error(s"Could not select database '$database' in $remote", e)
        }
        setNameFuture.recover {
          case e: Throwable => logger.error(s"Could not set client name in $remote", e)
        }
      }
    }
    case ReceiveTimeout =>
    case Terminated(_) => {
      isConnecting = false
      failAllQueuedRequests(RedisIOException(s"Could not connect to $remote"))
    }
  }
  
  def initializing: Receive = queue orElse {
    case Tcp.Received(data) => {
      val responsesCount = receive(data)
      initializationRequestsCount -= responsesCount
      if (initializationRequestsCount == 0) {
        onInitialized()
        sendAllQueuedRequests()
        if (isShuttingDown) {
          become(shuttingDown)
        } else {
          become(initialized)
        }
      }
    }
    case ReceiveTimeout => handleReceiveTimeout()
    case Terminated(_) => {
      logger.error(s"Could not initialize connection to $remote")
      failAllQueuedRequests(RedisIOException(s"Could not initialize connection to $remote"))
      reconnect()
    }
  }
  
  def initialized: Receive = send orElse {
    case Tcp.Received(data) => receive(data)
    case ReceiveTimeout => handleReceiveTimeout()
    case Terminated(_) => {
      logger.info("Connection has been shutdown abruptly")
      failAllSentRequests(RedisIOException("Connection has been shutdown abruptly"))
      reconnect()
    }
  }
  
  def reconnecting: Receive = queue orElse {
    case Tcp.Received(_) =>
    case ReceiveTimeout =>
    case Terminated(_) => {
      if (isReceiveTimeout) {
        logger.info(s"Connection has been reset due to receive timeout")
        failAllSentRequests(RedisIOException("Receive timeout"))
      } else {
        logger.info(s"Connection has been shutdown abruptly")
        failAllSentRequests(RedisIOException("Connection has been shutdown abruptly"))
      }
      reconnect()
    }
  }
  
  def shuttingDown: Receive = fail orElse {
    case Tcp.Received(data) => receive(data)
    case ReceiveTimeout => handleReceiveTimeout()
    case Shutdown => ioActor ! IOActor.ShutdownAck
    case Terminated(_) => shutdown()
  }
  
  def awaitingDecodersShutdown: Receive = fail orElse {
    case Terminated(_) => {
      decodersCount -= 1
      if (decodersCount == 0) {
        logger.info("Connection has been shutdown gracefully")
        context.stop(self)
      }
    }
  }
  
}

object ListenerActor {
  case object Connected
  case object Abort
  case object Shutdown
  case class Remove(count: Int)
}
