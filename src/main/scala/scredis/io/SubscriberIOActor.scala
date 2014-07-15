package scredis.io

import akka.actor._

import scredis.PubSubMessage
import scredis.protocol.requests.PubSubRequests.{ Subscribe, PSubscribe }
import scredis.exceptions.RedisIOException

import scala.collection.mutable.{ HashSet => MHashSet }

import java.net.InetSocketAddress

class SubscriberIOActor(
  remote: InetSocketAddress, passwordOpt: Option[String], database: Int
) extends IOActor(remote, passwordOpt, database) {
  
  private val subscribedChannels = MHashSet[String]()
  private val subscribedPatterns = MHashSet[String]()
  
  override protected def onConnect(): Unit = {
    // TODO: keep queued but do a PAUSE on ioActor until AUTH and RESUB
    failAllQueuedRequests(RedisIOException(s"Connection to $remote has been dropped"))
    partitionerActor ! PartitionerActor.ResetPubSub
    super.onConnect()
  }
  
  override protected def onAuthAndSelect(): Unit = {
    partitionerActor ! PartitionerActor.Subscribe(PartialFunction.empty)
    subscribedChannels.foreach { channel =>
      logger.info(s"Automatically re-subscribing to channel: $channel")
      val request = Subscribe(channel)
      this.requests.push(request)
      partitionerActor ! PartitionerActor.Push(request)
    }
    subscribedPatterns.foreach { pattern =>
      logger.info(s"Automatically re-subscribing to pattern: $pattern")
      val request = PSubscribe(pattern)
      this.requests.push(request)
      partitionerActor ! PartitionerActor.Push(request)
    }
    subscribedChannels.clear()
    subscribedPatterns.clear()
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
    case PubSubMessage.Unsubscribe(channelOpt, channelsCount) => channelOpt.foreach { channel =>
      logger.info(s"Unsubscribed from channel: $channel")
      subscribedChannels -= channel
    }
    case PubSubMessage.PUnsubscribe(patternOpt, patternsCount) => patternOpt.foreach { pattern =>
      logger.info(s"Unsubscribed from pattern: $pattern")
      subscribedPatterns -= pattern
    }
  }
  
}
