package scredis.io

import akka.actor._
import akka.routing.Broadcast
import akka.util.ByteString

import scredis.{ PubSubMessage, Subscription }
import scredis.protocol.Request
import scredis.protocol.requests.ConnectionRequests.{ Auth, Quit }
import scredis.protocol.requests.PubSubRequests
import scredis.exceptions._

import scala.collection.mutable.{ HashSet => MHashSet }
import scala.concurrent.duration._

import java.net.InetSocketAddress

class SubscriberListenerActor(
  host: String,
  port: Int,
  passwordOpt: Option[String],
  nameOpt: Option[String],
  decodersCount: Int,
  receiveTimeoutOpt: Option[FiniteDuration],
  connectTimeout: FiniteDuration,
  maxWriteBatchSize: Int,
  tcpSendBufferSizeHint: Int,
  tcpReceiveBufferSizeHint: Int,
  akkaIODispatcherPath: String,
  akkaDecoderDispatcherPath: String
) extends ListenerActor(
  host = host,
  port = port,
  passwordOpt = passwordOpt,
  database = 0,
  nameOpt = nameOpt,
  decodersCount = decodersCount,
  receiveTimeoutOpt = receiveTimeoutOpt,
  connectTimeout = connectTimeout,
  maxWriteBatchSize = maxWriteBatchSize,
  tcpSendBufferSizeHint = tcpSendBufferSizeHint,
  tcpReceiveBufferSizeHint = tcpReceiveBufferSizeHint,
  akkaIODispatcherPath = akkaIODispatcherPath,
  akkaDecoderDispatcherPath = akkaDecoderDispatcherPath
) {
  
  import SubscriberListenerActor._
  
  private val subscribedChannels = MHashSet[String]()
  private val subscribedPatterns = MHashSet[String]()
  
  private val savedSubscribedChannels = MHashSet[String]()
  private val savedSubscribedPatterns = MHashSet[String]()
  
  private var isInitialized = false
  private var shouldSendRequests = false
  private var requestOpt: Option[Request[_]] = None
  private var requestResponsesCount = 0
  private var subscriptionOpt: Option[Subscription] = None
  private var previousSubscriptionOpt: Option[Subscription] = None
  private var subscribedCount = 0
  private var subscribedChannelsCount = 0
  private var subscribedPatternsCount = 0
  
  override protected def onConnect(): Unit = {
    isInitialized = false
    shouldSendRequests = false
    previousSubscriptionOpt = subscriptionOpt
    subscriptionOpt = None
    requestOpt = None
    requestResponsesCount = 0
    subscribedCount = 0
    subscribedChannelsCount = 0
    subscribedPatternsCount = 0
  }
  
  override protected def onInitialized(): Unit = {
    isInitialized = true
    val subscription = previousSubscriptionOpt.getOrElse(PartialFunction.empty)
    subscriptionOpt = Some(subscription)
    decoders.route(Broadcast(DecoderActor.Subscribe(subscription)), self)
    
    subscribedChannels.foreach { channel =>
      logger.info(s"Automatically re-subscribing to channel: $channel")
      val request = PubSubRequests.Subscribe(channel)
      if (shouldSendRequests) {
        send(request)
      } else {
        queuedRequests.push(request)
      }
    }
    subscribedPatterns.foreach { pattern =>
      logger.info(s"Automatically re-subscribing to pattern: $pattern")
      val request = PubSubRequests.PSubscribe(pattern)
      if (shouldSendRequests) {
        send(request)
      } else {
        queuedRequests.push(request)
      }
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
      if (isInitialized) {
        decoders.route(Broadcast(DecoderActor.Subscribe(subscription)), self)
      }
    }
    case Complete(message) => {
      requestResponsesCount += 1
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
        case x => throw RedisProtocolException(s"Unexpected pub sub message received: $x")
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
          case x => throw RedisProtocolException(
            s"Unexpected pub sub message received: '$x' in response to '$request'"
          )
        }
        if (count == otherCount) {
          request.success(count)
          requestOpt = None
          requestResponsesCount = 0
        } else {
          requestOpt = Some(request)
        }
      } else {
        if (argsCount == requestResponsesCount) {
          request.success(count)
          requestOpt = None
          requestResponsesCount = 0
        } else {
          requestOpt = Some(request)
        }
      }
    }
    case Fail(message) => requests.pop().failure(RedisErrorResponseException(message))
    case SaveSubscriptions => {
      savedSubscribedChannels ++= subscribedChannels
      savedSubscribedPatterns ++= subscribedPatterns
    }
    case SendAsRegularClient(request) => {
      onConnect()
      send(request)
    }
    case RecoverPreviousSubscriberState => {
      shouldSendRequests = true
      subscribedChannels ++= savedSubscribedChannels
      subscribedPatterns ++= savedSubscribedPatterns
      savedSubscribedChannels.clear()
      savedSubscribedPatterns.clear()
      onInitialized()
      shouldSendRequests = false
    }
    case Shutdown(quit) => {
      onConnect()
      send(quit)
    }
  }
  
}

object SubscriberListenerActor {
  case class Subscribe(subscription: Subscription)
  case class Complete(message: PubSubMessage)
  case class Fail(message: String)
  case object SaveSubscriptions
  case class SendAsRegularClient(request: Request[_])
  case object RecoverPreviousSubscriberState
  case class Shutdown(quit: Quit)
}
