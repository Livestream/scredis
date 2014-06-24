package scredis.nio
/*
import com.codahale.metrics.MetricRegistry

import akka.actor.{ Actor, ActorRef }

import scredis.util.Logger
import scredis.protocol.Request

class EncoderActor(decoderActor: ActorRef) extends Actor {
  
  private val logger = Logger(getClass)
  /*
  private val encodeTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "encodeTimer")
  )
  private val tellTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "tellTimer")
  )*/
  
  def receive: Receive = {
    case request: Request[_] => {
      scredis.protocol.NioProtocol.concurrent.acquire()
      logger.trace(request.toString)
      //val encode = encodeTimer.time()
      //request.encode()
      //encode.stop()
      //val tell = tellTimer.time()
      decoderActor ! request
      //tell.stop()
    }
  }
  
}*/