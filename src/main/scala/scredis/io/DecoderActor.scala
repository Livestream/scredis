package scredis.io

import com.typesafe.scalalogging.LazyLogging

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.{ PubSubMessage, Subscription }
import scredis.protocol.{ Protocol, Request, ErrorResponse }
import scredis.exceptions.RedisProtocolException

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue }
import scala.concurrent.{ ExecutionContext, Future }

import java.nio.ByteBuffer

class DecoderActor extends Actor with LazyLogging {
  
  import DecoderActor._
  
  private var subscriptionOpt: Option[Subscription] = None
  
  def receive: Receive = {
    case Partition(data, requests, skip) => {
      val buffer = data.asByteBuffer
      
      for (i <- 1 to skip) {
        try {
          Protocol.decode(buffer)
        } catch {
          case e: Throwable => logger.error("Could not decode response", e)
        }
      }
      
      while (requests.hasNext) {
        val request = requests.next()
        try {
          val response = Protocol.decode(buffer)
          request.complete(response)
        } catch {
          case e: Throwable => {
            logger.error("Could not decode response", e)
            request.failure(RedisProtocolException("Could not decode response", e))
          }
        }
      }
    }
    case SubscribePartition(data) => {
      val buffer = data.asByteBuffer
      while (buffer.remaining > 0) {
        try {
          val result = Protocol.decodePubSubResponse(Protocol.decode(buffer))
          result match {
            case Left(ErrorResponse(message)) => {
              sender ! SubscriberListenerActor.Fail(message)
            }
            case Right(m: PubSubMessage.Subscribe) => {
              sender ! SubscriberListenerActor.Complete(m)
            }
            case Right(m: PubSubMessage.PSubscribe) => {
              sender ! SubscriberListenerActor.Complete(m)
            }
            case Right(m: PubSubMessage.Unsubscribe) => {
              sender ! SubscriberListenerActor.Complete(m)
            }
            case Right(m: PubSubMessage.PUnsubscribe) => {
              sender ! SubscriberListenerActor.Complete(m)
            }
            case _ =>
          }
          result match {
            case Left(_) =>
            case Right(message) => subscriptionOpt match {
              case Some(subscription) => if (subscription.isDefinedAt(message)) {
                Future {
                  subscription.apply(message)
                }(ExecutionContext.global)
              } else {
                logger.debug(s"Received unregistered PubSubMessage: $message")
              }
              case None => logger.error("Received SubscribePartition without any subscription")
            }
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
  case class Partition(data: ByteString, requests: Iterator[Request[_]], skip: Int)
  case class SubscribePartition(data: ByteString)
  case class Subscribe(subscription: Subscription)
}
