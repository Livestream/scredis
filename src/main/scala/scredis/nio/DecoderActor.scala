package scredis.nio

import com.codahale.metrics.MetricRegistry

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.util.Logger
import scredis.protocol.{ NioProtocol, Request }

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }

import java.nio.ByteBuffer

class DecoderActor extends Actor {
  
  private val logger = Logger(getClass)
  private val requests = MQueue[Request[_]]()
  
  private var count = 0
  
  private val decodeTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "decodeTimer")
  )
  
  def receive: Receive = {
    case p @ Partition(data, requests) => {
      logger.trace(s"Decoding ${requests.size} requests")
      val buffer = data.asByteBuffer
      val decode = decodeTimer.time()
      while (requests.hasNext) {
        val reply = NioProtocol.decode(buffer)
        requests.next().complete(Success(reply))
        count += 1
        if (count % 100000 == 0) logger.info(count.toString)
      }
      decode.stop()
    }
  }
  
}
