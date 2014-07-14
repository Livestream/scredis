package scredis.commands

import scredis.Subscription
import scredis.io.{ Connection, SubscriberConnection }
import scredis.protocol.requests.PubSubRequests._

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
   * @param pf partial function handling received messages
   *
   * @since 2.0.0
   */
  def pSubscribe(patterns: String*)(subscription: Subscription): Unit = {
    updateSubscription(subscription)
    if (!patterns.isEmpty) {
      sendAsSubscriber(PSubscribe(patterns: _*))
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
   *
   * @since 2.0.0
   */
  def pUnsubscribe(patterns: String*): Unit = sendAsSubscriber(PUnsubscribe(patterns: _*))
  
  /**
   * Listens for messages published to the given channels.
   *
   * @note Once the client enters the subscribed state it is not supposed to issue any other
   * commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * @param channels channel name(s) of channel(s) to listen to
   * @param subscription partial function handling received messages
   *
   * @since 2.0.0
   */
  def subscribe(channels: String*)(subscription: Subscription): Unit = {
    updateSubscription(subscription)
    if (!channels.isEmpty) {
      sendAsSubscriber(Subscribe(channels: _*))
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
   *
   * @since 2.0.0
   */
  def unsubscribe(channels: String*): Unit = sendAsSubscriber(Unsubscribe(channels: _*))
  
}