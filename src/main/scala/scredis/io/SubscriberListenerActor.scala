package scredis.io

import akka.actor._
import akka.routing.Broadcast
import akka.util.ByteString

import scredis.{ PubSubMessage, Subscription }
import scredis.protocol.Request
import scredis.protocol.requests.ConnectionRequests.Quit
import scredis.protocol.requests.PubSubRequests
import scredis.exceptions.RedisIOException

import scala.collection.mutable.{ HashSet => MHashSet }
import scala.concurrent.duration._

import java.net.InetSocketAddress

class SubscriberListenerActor(
  host: String,
  port: Int,
  passwordOpt: Option[String],
  database: Int,
  decodersCount: Int,
  receiveTimeout: FiniteDuration
) extends ListenerActor(host, port, passwordOpt, database, decodersCount, receiveTimeout) {
  
  import SubscriberListenerActor._
  
  private val subscribedChannels = MHashSet[String]()
  private val subscribedPatterns = MHashSet[String]()
  
  private var isInitialized = false
  private var requestOpt: Option[Request[_]] = None
  private var requestRepliesCount = 0
  private var subscriptionOpt: Option[Subscription] = None
  private var previousSubscriptionOpt: Option[Subscription] = None
  private var subscribedCount = 0
  private var subscribedChannelsCount = 0
  private var subscribedPatternsCount = 0
  
  override protected def onConnect(): Unit = {
    previousSubscriptionOpt = subscriptionOpt
    subscriptionOpt = None
    requestOpt = None
    requestRepliesCount = 0
    subscribedCount = 0
    subscribedChannelsCount = 0
    subscribedPatternsCount = 0
  }
  
  override protected def onInitialized(): Unit = {
    println("CALLED", previousSubscriptionOpt, subscribedChannels, subscribedPatterns)
    val subscription = previousSubscriptionOpt.getOrElse(PartialFunction.empty)
    subscriptionOpt = Some(subscription)
    decoders.route(Broadcast(DecoderActor.Subscribe(subscription)), self)
    
    subscribedChannels.foreach { channel =>
      logger.info(s"Automatically re-subscribing to channel: $channel")
      val request = PubSubRequests.Subscribe(channel)
      queuedRequests.push(request)
    }
    subscribedPatterns.foreach { pattern =>
      logger.info(s"Automatically re-subscribing to pattern: $pattern")
      val request = PubSubRequests.PSubscribe(pattern)
      queuedRequests.push(request)
    }
    subscribedChannels.clear()
    subscribedPatterns.clear()
  }
  
  override protected def handleData(
    data: ByteString, responsesCount: Int
  ): Unit = subscriptionOpt match {
    case Some(_) => decoders.route(DecoderActor.SubscribePartition(data), self)
    case None => super.handleData(data, responsesCount)
  }
  
  override protected def always: Receive = super.always orElse {
    case Subscribe(subscription) => {
      subscriptionOpt = Some(subscription)
      decoders.route(Broadcast(DecoderActor.Subscribe(subscription)), self)
    }
    case Complete(message) => {
      requestRepliesCount += 1
      val count = message match {
        case PubSubMessage.Subscribe(channel, count) => {
          logger.info(s"Subscribed to channel: $channel")
          subscribedChannels += channel
          subscribedCount += 1
          subscribedChannelsCount += 1
          count
        }
        case PubSubMessage.PSubscribe(pattern, count) => {
          logger.info(s"Subscribed to pattern: $pattern")
          subscribedPatterns += pattern
          subscribedCount += 1
          subscribedPatternsCount += 1
          count
        }
        case PubSubMessage.Unsubscribe(channelOpt, count) => {
          channelOpt.foreach { channel =>
            logger.info(s"Unsubscribed from channel: $channel")
            subscribedChannels -= channel
          }
          val difference = subscribedCount - count
          subscribedCount -= difference
          subscribedChannelsCount -= difference
          count
        }
        case PubSubMessage.PUnsubscribe(patternOpt, count) => {
          patternOpt.foreach { pattern =>
            logger.info(s"Unsubscribed from pattern: $pattern")
            subscribedPatterns -= pattern
          }
          val difference = subscribedCount - count
          subscribedCount -= difference
          subscribedPatternsCount -= difference
          count
        }
        case _ => ???
      }
      
      val (request, argsCount) = requestOpt match {
        case Some(request) => (request, request.argsCount)
        case None => {
          val request = requests.pop()
          val argsCount = request.argsCount
          (request, argsCount)
        }
      }
      // Unsubscribe() or PUnsubscribe()
      if (argsCount == 0) {
        val (count, otherCount) = message match {
          case PubSubMessage.Unsubscribe(_, count) => (count, subscribedPatternsCount)
          case PubSubMessage.PUnsubscribe(_, count) => (count, subscribedChannelsCount)
          case x => ???
        }
        if (count == otherCount) {
          request.success(count)
          requestOpt = None
          requestRepliesCount = 0
        } else {
          requestOpt = Some(request)
        }
      } else {
        if (argsCount == requestRepliesCount) {
          request.success(count)
          requestOpt = None
          requestRepliesCount = 0
        } else {
          requestOpt = Some(request)
        }
      }
    }
    case Shutdown(quit) => {
      onConnect()
      self ! quit
    }
  }
  
}

object SubscriberListenerActor {
  case class Subscribe(subscription: Subscription)
  case class Complete(message: PubSubMessage)
  case class Shutdown(quit: Quit)
}
