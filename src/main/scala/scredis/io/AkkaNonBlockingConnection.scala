package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor._

import scredis.Transaction
import scredis.exceptions._
import scredis.protocol._
import scredis.util.UniqueNameGenerator

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * This trait represents a non-blocking connection to a `Redis` server.
 */
abstract class AkkaNonBlockingConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  nameOpt: Option[String],
  decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaListenerDispatcherPath: String,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String
) extends AbstractAkkaConnection(
  system = system,
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = database,
  nameOpt = nameOpt,
  decodersCount = decodersCount,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
) with NonBlockingConnection with TransactionEnabledConnection {
  
  private val lock = new ReentrantReadWriteLock()
  
  protected implicit val listenerActor = system.actorOf(
    Props(
      classOf[ListenerActor],
      host,
      port,
      passwordOpt,
      database,
      nameOpt,
      decodersCount,
      receiveTimeoutOpt,
      connectTimeout,
      maxWriteBatchSize,
      tcpSendBufferSizeHint,
      tcpReceiveBufferSizeHint,
      akkaIODispatcherPath,
      akkaDecoderDispatcherPath
    ).withDispatcher(akkaListenerDispatcherPath),
    UniqueNameGenerator.getUniqueName(s"${nameOpt.getOrElse(s"$host-$port")}-listener-actor")
  )
  
  override protected def send[A](request: Request[A]): Future[A] = {
    if (isShuttingDown) {
      Future.failed(RedisIOException("Connection has been shutdown"))
    } else {
      logger.debug(s"Sending request: $request")
      updateState(request)
      Protocol.send(request)
    }
  }
  
  override protected def send(transaction: Transaction): Future[Vector[Try[Any]]] = {
    if (isShuttingDown) {
      Future.failed(RedisIOException("Connection has been shutdown"))
    } else {
      logger.debug(s"Sending transaction: $transaction")
      transaction.requests.foreach(updateState)
      Protocol.send(transaction)
    }
  }
  
}