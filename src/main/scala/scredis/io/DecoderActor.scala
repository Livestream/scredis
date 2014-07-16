package scredis.io

import com.codahale.metrics.MetricRegistry

import com.typesafe.scalalogging.slf4j.LazyLogging

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.{ PubSubMessage, Subscription }
import scredis.protocol.{ Protocol, Request }
import scredis.exceptions.RedisProtocolException

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }
import scala.concurrent.{ ExecutionContext, Future }

import java.nio.ByteBuffer

class DecoderActor(ioActor: ActorRef) extends Actor with LazyLogging {
  
  import DecoderActor._
  
  private var subscriptionOpt: Option[Subscription] = None
  private var count = 0
  
  private val decodeTimer = scredis.protocol.Protocol.metrics.timer(
    MetricRegistry.name(getClass, "decodeTimer")
  )
  
  def receive: Receive = {
    case Partition(data, requests) => {
      val buffer = data.asByteBuffer
      val decode = decodeTimer.time()
      while (requests.hasNext) {
        val request = requests.next()
        try {
          val response = Protocol.decode(buffer)
          request.complete(response)
        } catch {
          case e: Throwable => request.failure(
            RedisProtocolException("Could not decode response", e)
          )
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
          message match {
            case m: PubSubMessage.Subscribe => {
              ioActor ! m
              sender ! PartitionerActor.Complete(m)
            }
            case m: PubSubMessage.PSubscribe => {
              ioActor ! m
              sender ! PartitionerActor.Complete(m)
            }
            case m: PubSubMessage.Unsubscribe => {
              ioActor ! m
              sender ! PartitionerActor.Complete(m)
            }
            case m: PubSubMessage.PUnsubscribe => {
              ioActor ! m
              sender ! PartitionerActor.Complete(m)
            }
            case _ =>
          }
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
          case e: Throwable => logger.error(
            s"Could not decode PubSubMessage: " 
              +s"${data.decodeString("UTF-8").replace("\r\n", "\\r\\n")}",
            e
          )
        }
      }
    }
    case Subscribe(subscription) => subscriptionOpt = Some(subscription)
    case x => logger.error(s"Received unexpected message: $x")
  }
  
}

object DecoderActor {
  case class Partition(data: ByteString, requests: Iterator[Request[_]])
  case class SubscribePartition(data: ByteString)
  case class Subscribe(subscription: Subscription)
}
