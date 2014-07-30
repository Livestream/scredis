package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.protocol.Request
import scredis.protocol.requests.ConnectionRequests.{ Auth, Select, Quit }
import scredis.protocol.requests.ServerRequests.{ ClientSetName, Shutdown }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

abstract class AbstractAkkaConnection(
  protected val system: ActorSystem,
  protected val host: String,
  protected val port: Int,
  @volatile protected var passwordOpt: Option[String],
  @volatile protected var database: Int,
  @volatile protected var nameOpt: Option[String],
  protected val decodersCount: Int,
  protected val receiveTimeoutOpt: Option[FiniteDuration],
  protected val connectTimeout: FiniteDuration,
  protected val maxWriteBatchSize: Int,
  protected val tcpSendBufferSizeHint: Int,
  protected val tcpReceiveBufferSizeHint: Int,
  protected val akkaListenerDispatcherPath: String,
  protected val akkaIODispatcherPath: String,
  protected val akkaDecoderDispatcherPath: String
) extends Connection with LazyLogging {
  
  @volatile protected var isShuttingDown = false
  
  override implicit val dispatcher = system.dispatcher
  
  protected def updateState(request: Request[_]): Unit = request match {
    case Auth(password) => if (password.isEmpty) {
      passwordOpt = None
    } else {
      passwordOpt = Some(password)
    }
    case Select(database) => this.database = database
    case ClientSetName(name) => if (name.isEmpty) {
      nameOpt = None
    } else {
      nameOpt = Some(name)
    }
    case Quit() | Shutdown(_) => isShuttingDown = true
    case _            =>
  }
  
  protected def getPasswordOpt: Option[String] = passwordOpt
  protected def getDatabase: Int = database
  protected def getNameOpt: Option[String] = nameOpt
  
}