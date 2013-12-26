package scredis.nio

import akka.actor.{ Actor, ActorRef }

import scredis.util.Logger
import scredis.protocol.Request

class EncoderActor(decoderActor: ActorRef) extends Actor {
  
  private val logger = Logger(getClass)
  
  def receive: Receive = {
    case request: Request[_] => {
      logger.trace(request.toString)
      request.encode()
      decoderActor ! request
    }
  }
  
}