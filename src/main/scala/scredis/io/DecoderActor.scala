package scredis.io

import com.codahale.metrics.MetricRegistry

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.Subscription
import scredis.protocol.{ Protocol, Request }
import scredis.exceptions.RedisProtocolException

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }
import scala.concurrent.{ ExecutionContext, Future }

import java.nio.ByteBuffer

class DecoderActor extends Actor with LazyLogging {
  
  import DecoderActor._
  
  private var subscriptionOpt: Option[Subscription] = None
  private var count = 0
  
  private val decodeTimer = scredis.protocol.Protocol.metrics.timer(
    MetricRegistry.name(getClass, "decodeTimer")
  )
  
  def receive: Receive = {
    case Partition(data, requests, skip) => {
      var skipCount = skip
      val buffer = data.asByteBuffer
      val decode = decodeTimer.time()
      while (requests.hasNext) {
        val request = requests.next()
        try {
          val response = Protocol.decode(buffer)
          if (skipCount == 0) {
            request.complete(response)
          } else {
            skipCount -= 1
          }
        } catch {
          case e: Throwable => {
            if (skipCount == 0) {
              request.failure(RedisProtocolException("Could not decode response", e))
            } else {
              skipCount -= 1
            }
          }
        }
        count += 1
        if (count % 100000 == 0) println(count)
      }
      decode.stop()
    }
    case SubscribePartition(data) => {
      val buffer = data.asByteBuffer
      while (buffer.remaining > 0) {
        try {
          val message = Protocol.decodePubSubResponse(Protocol.decode(buffer))
          subscriptionOpt match {
            case Some(subscription) => if (subscription.isDefinedAt(message)) {
              Future {
                subscription.apply(message)
              }(ExecutionContext.global)
            } else {
              logger.debug(s"Received unregistered PubSubMessage: $message")
            }
            case None => logger.error("Received SubscribePartition without any subscription")
          }
        } catch {
          case e: Throwable => logger.error("Could not decode PubSubMessage", e)
        }
      }
    }
    case Subscribe(subscription) => subscriptionOpt = Some(subscription)
  }
  
}

object DecoderActor {
  case class Partition(data: ByteString, requests: Iterator[Request[_]], skip: Int)
  case class SubscribePartition(data: ByteString)
  case class Subscribe(subscription: Subscription)
}
