package scredis.commands

import scredis.io.Connection
import scredis.protocol.requests.PubSubRequests._
import scredis.serialization.Writer

import scala.concurrent.Future

/**
 * This trait implements pub/sub commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait PubSubCommands { self: Connection =>
  
  /**
   * Publishes a message to a channel.
   *
   * @param channel the name of the channel
   * @param message the message payload
   * @return the number of clients that received the message
   *
   * @since 2.0.0
   */
  def publish[W: Writer](channel: String, message: W): Future[Long] = send(
    Publish(channel, message)
  )
  
  /**
   * Lists the currently active channels. An active channel is a Pub/Sub channel with one or more
   * subscribers (not including clients subscribed to patterns).
   * 
   * @note If no pattern is specified, all the channels are listed, otherwise if pattern is
   * specified only channels matching the specified glob-style pattern are listed.
   *
   * @param patternOpt optional pattern to filter returned channels
   * @return the currently active channels, optionally matching the specified pattern
   *
   * @since 2.8.0
   */
  def pubSubChannels(patternOpt: Option[String] = None): Future[List[String]] = send(
    PubSubChannels[List](patternOpt)
  )
  
  /**
   * Returns the number of subscribers (not counting clients subscribed to patterns) for the
   * specified channels.
   *
   * @param channels channel name(s)
   * @return a map of channels to number of subscribers for every provided channel
   *
   * @since 2.8.0
   */
  def pubSubNumSub(channels: String*): Future[Map[String, Int]] = send(
    PubSubNumSub[Map](channels: _*)
  )
  
  /**
   * Returns the number of subscriptions to patterns (that are performed using the
   * PSUBSCRIBE command).
   * 
   * @note Note that this is not just the count of clients subscribed to patterns but the total
   * number of patterns all the clients are subscribed to.
   *
   * @return the number of subscriptions to patterns
   *
   * @since 2.8.0
   */
  def pubSubNumPat(): Future[Long] = send(PubSubNumPat())
  
}