package scredis.io

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor._

import scredis.protocol.Request
import scredis.protocol.requests.ConnectionRequests.{ Auth, Select, Quit }
import scredis.protocol.requests.ServerRequests.Shutdown

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

abstract class AbstractAkkaConnection(
  protected val system: ActorSystem,
  protected val host: String,
  protected val port: Int,
  protected var passwordOpt: Option[String],
  protected var database: Int,
  protected val decodersCount: Int = 3,
  protected val receiveTimeoutOpt: Option[FiniteDuration] = Some(5 seconds)
) extends Connection with LazyLogging {
  
  @volatile protected var isShuttingDown = false
  
  override protected implicit val ec = system.dispatcher
  
  protected def updateState(request: Request[_]): Unit = request match {
    case Auth(password) => if (password.isEmpty) {
      passwordOpt = None
    } else {
      passwordOpt = Some(password)
    }
    case Select(database) => this.database = database
    case Quit() | Shutdown(_) => isShuttingDown = true
    case _            =>
  }
  
}