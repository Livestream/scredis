package scredis.io

import com.codahale.metrics.MetricRegistry

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.protocol.{ Protocol, Request }

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }

import java.nio.ByteBuffer

class DecoderActor extends Actor {
  
  import DecoderActor.Partition
  
  private var count = 0
  
  private val decodeTimer = scredis.protocol.Protocol.metrics.timer(
    MetricRegistry.name(getClass, "decodeTimer")
  )
  
  def receive: Receive = {
    case p @ Partition(data, requests) => {
      val buffer = data.asByteBuffer
      val decode = decodeTimer.time()
      while (requests.hasNext) {
        val response = Protocol.decode(buffer)
        requests.next().complete(response)
        count += 1
        if (count % 100000 == 0) println(count)
      }
      decode.stop()
    }
  }
  
}

object DecoderActor {
  case class Partition(data: ByteString, requests: Iterator[Request[_]])
}
