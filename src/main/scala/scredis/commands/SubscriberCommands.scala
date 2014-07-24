package scredis.commands

import scredis.Subscription
import scredis.io.{ Connection, SubscriberConnection }
import scredis.protocol.requests.PubSubRequests._
import scredis.exceptions.RedisInvalidArgumentException

import scala.concurrent.Future

/**
 * This trait implements subscriber commands.
 */
trait SubscriberCommands { self: SubscriberConnection =>

  /**
   * Listens for messages published to channels matching the given patterns.
   *
   * @note Once the client enters the subscribed state it is not supposed to issue any other
   * commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * @param patterns the patterns
   * @param subscription partial function handling received messages
   * @return the total number of subscribed channels and patterns
   *
   * @since 2.0.0
   */
  def pSubscribe(patterns: String*)(subscription: Subscription): Future[Int] = {
    setSubscription(subscription)
    if (!patterns.isEmpty) {
      sendAsSubscriber(PSubscribe(patterns: _*))
    } else {
      Future.failed(RedisInvalidArgumentException("PSUBSCRIBE: patterns cannot be empty"))
    }
  }
  
  /**
   * Stops listening for messages published to channels matching the given patterns.
   *
   * @note When no patterns are specified, the client is unsubscribed from all the previously
   * subscribed patterns. In this case, a message for every unsubscribed pattern will be sent to
   * the client.
   *
   * @param patterns the patterns, if empty, unsubscribe from all patterns
   * @return the total number of subscribed channels and patterns
   *
   * @since 2.0.0
   */
  def pUnsubscribe(patterns: String*): Future[Int] = sendAsSubscriber(PUnsubscribe(patterns: _*))
  
  /**
   * Listens for messages published to the given channels.
   *
   * @note Once the client enters the subscribed state it is not supposed to issue any other
   * commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * @param channels channel name(s) of channel(s) to listen to
   * @param subscription partial function handling received messages
   * @return the total number of subscribed channels and patterns
   *
   * @since 2.0.0
   */
  def subscribe(channels: String*)(subscription: Subscription): Future[Int] = {
    setSubscription(subscription)
    if (!channels.isEmpty) {
      sendAsSubscriber(Subscribe(channels: _*))
    } else {
      Future.failed(RedisInvalidArgumentException("SUBSCRIBE: channels cannot be empty"))
    }
  }

  /**
   * Stops listening for messages published to the given channels.
   *
   * @note When no channels are specified, the client is unsubscribed from all the previously
   * subscribed channels. In this case, a message for every unsubscribed channel will be sent to
   * the client.
   *
   * @param channels the names of the channels, if empty, unsubscribe from all channels
   * @return the total number of subscribed channels and patterns
   *
   * @since 2.0.0
   */
  def unsubscribe(channels: String*): Future[Int] = sendAsSubscriber(Unsubscribe(channels: _*))
  
}