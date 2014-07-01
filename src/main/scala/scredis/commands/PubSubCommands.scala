package scredis.commands

import scredis.AbstractClient
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
trait PubSubCommands { self: AbstractClient =>
  
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
  
}