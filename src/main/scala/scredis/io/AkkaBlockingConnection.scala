package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor._

import scredis.Transaction
import scredis.exceptions._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.ServerRequests.Shutdown
import scredis.util.UniqueNameGenerator

import scala.util.Try
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

/**
 * This trait represents a blocking connection to a `Redis` server.
 */
abstract class AkkaBlockingConnection(
  system: ActorSystem,
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  nameOpt: Option[String],
  decodersCount: Int,
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
  receiveTimeoutOpt = None,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaListenerDispatcherPath = akkaListenerDispatcherPath,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
) with BlockingConnection {
  
  private val lock = new ReentrantLock()
  
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
  
  private def withLock[A](f: => A): A = {
    if (lock.tryLock) {
      try {
        f
      } finally {
        lock.unlock()
      }
    } else {
      throw RedisIOException("Trying to send request on a blocked connection")
    }
  }
  
  override protected def sendBlocking[A](request: Request[A])(
    implicit timeout: Duration
  ): Try[A] = withLock {
    logger.debug(s"Sending blocking request: $request")
    updateState(request)
    val future = Protocol.send(request)
    Try(Await.result(future, timeout))
  }
  
}