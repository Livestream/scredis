package scredis.io

import akka.actor.Actor
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import scredis.exceptions.RedisProtocolException
import scredis.protocol.{ErrorResponse, Protocol, Request}
import scredis.{PubSubMessage, Subscription}

import scala.collection.mutable.{Queue => MQueue}
import scala.concurrent.{ExecutionContext, Future}

class DecoderActor extends Actor with LazyLogging {
  
  import scredis.io.DecoderActor._
  
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
                logger.debug("Received unregistered PubSubMessage: %s", message)
              }
              case None => logger.error("Received SubscribePartition without any subscription")
            }
          }
        } catch {
          case e: Throwable =>
            val decoded = data.decodeString("UTF-8").replace("\r\n", "\\r\\n")
            logger.error(s"Could not decode PubSubMessage: $decoded", e)
        }
      }
    }
    case Subscribe(subscription) => subscriptionOpt = Some(subscription)
    case x: AnyRef => logger.error("Received unexpected message: %s", x)
  }
  
}

object DecoderActor {
  case class Partition(data: ByteString, requests: Iterator[Request[_]], skip: Int)
  case class SubscribePartition(data: ByteString)
  case class Subscribe(subscription: Subscription)
}
