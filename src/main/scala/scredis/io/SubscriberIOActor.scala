package scredis.io

import akka.actor._

import scredis.PubSubMessage
import scredis.protocol.requests.PubSubRequests.{ Subscribe, PSubscribe }

import scala.collection.mutable.{ HashSet => MHashSet }

import java.net.InetSocketAddress

class SubscriberIOActor(
  remote: InetSocketAddress, passwordOpt: Option[String], database: Int
) extends IOActor(remote, passwordOpt, database) {
  
  private val subscribedChannels = MHashSet[String]()
  private val subscribedPatterns = MHashSet[String]()
  
  override protected def onConnect(): Unit = {
    subscribedChannels.foreach { channel =>
      logger.info(s"Automatically re-subscribing to channel: $channel")
      val request = Subscribe(channel)
      this.requests.push(request)
    }
    subscribedPatterns.foreach { pattern =>
      logger.info(s"Automatically re-subscribing to pattern: $pattern")
      val request = PSubscribe(pattern)
      this.requests.push(request)
    }
    subscribedChannels.clear()
    subscribedPatterns.clear()
    super.onConnect()
  }
  
  override protected def all: Receive = {
    case PubSubMessage.Subscribe(channel, channelsCount) => {
      logger.info(s"Subscribed to channel: $channel")
      subscribedChannels += channel
    }
    case PubSubMessage.PSubscribe(pattern, patternsCount) => {
      logger.info(s"Subscribed to pattern: $pattern")
      subscribedPatterns += pattern
    }
    case PubSubMessage.Unsubscribe(Some(channel), channelsCount) => {
      logger.info(s"Unsubscribed from channel: $channel")
      subscribedChannels -= channel
    }
    case PubSubMessage.PUnsubscribe(Some(pattern), patternsCount) => {
      logger.info(s"Unsubscribed from pattern: $pattern")
      subscribedPatterns -= pattern
    }
  }
  
}
