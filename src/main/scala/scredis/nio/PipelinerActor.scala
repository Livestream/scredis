package scredis.nio
/*
import akka.actor.{ Actor, ActorRef }

import scredis.util.Logger
import scredis.protocol.Request

import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable.{ Queue => MQueue, ListBuffer }

class PipelinerActor(
  ioActor: ActorRef,
  interval: FiniteDuration,
  thresholdOpt: Option[Int]
) extends Actor {
  
  import context.dispatcher
  
  private val logger = Logger(getClass)
  private val requests = MQueue[Request[_]]()
  
  private def addOrSend(request: Request[_]): Unit = {
    logger.trace(request.toString)
    if (true) {
      requests += request
    } else {
      ioActor ! request
    }
  }
  
  private def sendAll(): Unit = if (!requests.isEmpty) {
    logger.trace(s"Sending ${requests.size} requests")
    ioActor ! requests.toList
    requests.clear()
  }
  
  private val requestPf: Receive = thresholdOpt match {
    case Some(threshold) => {
      case request: Request[_] => {
        addOrSend(request)
        if (requests.size >= threshold) {
          sendAll()
        }
      }
    }
    case None => {
      case request: Request[_] => addOrSend(request)
    }
  }
  
  private val intervalPf: Receive = {
    case 'Send => sendAll()
  }
  
  def receive: Receive = requestPf orElse intervalPf
  
  def ready: Receive = {
    case request: Request[_] => ioActor ! request
  }
  
  def buffering: Receive = {
    case request: Request[_] => requests.enqueue(request)
    case 'Ready => {
      val requests = ListBuffer[Request[_]]()
      var i = 0
      while (!this.requests.isEmpty && i < 1000) {
        requests += this.requests.dequeue()
        i += 1
      }
      self ! requests.toList
      become(ready)
    }
  }
  
  context.system.scheduler.schedule(interval, interval, self, 'Send)
  
}*/