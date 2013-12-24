package scredis.nio

import akka.actor.{ Actor, ActorRef }

import scredis.protocol.Request

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.ListBuffer

class PipelinerActor(
  ioActor: ActorRef,
  interval: FiniteDuration,
  thresholdOpt: Option[Int]
) extends Actor {
  
  import context.dispatcher
  
  private val requests = ListBuffer[Request[_, _]]()
  
  private def addOrSend(request: Request[_, _]): Unit = {
    if (true) {
      requests += request
    } else {
      ioActor ! request
    }
  }
  
  private def sendAll(): Unit = if (!requests.isEmpty) {
    ioActor ! requests.toList
    requests.clear()
  }
  
  private val requestPf: Receive = thresholdOpt match {
    case Some(threshold) => {
      case request: Request[_, _] => {
        addOrSend(request)
        if (requests.size >= threshold) {
          sendAll()
        }
      }
    }
    case None => {
      case request: Request[_, _] => addOrSend(request)
    }
  }
  
  private val intervalPf: Receive = {
    case 'Send => sendAll()
  }
  
  def receive: Receive = requestPf orElse intervalPf
  
  context.system.scheduler.schedule(interval, interval, self, 'Send)
  
}