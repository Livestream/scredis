package scredis.pubsub

/**
 * Base trait for all pub/sub messages.
 */
sealed trait PubSubMessage

/**
 * This message is triggered after we successfully subscribed to a channel.
 */
final case class Subscribe(channel: String, channelsCount: Int) extends PubSubMessage

/**
 * This message is triggered after we successfully subscribed to a pattern.
 */
final case class PSubscribe(pattern: String, patternsCount: Int) extends PubSubMessage

/**
 * This message is triggered after we successfully unsubscribed from a channel.
 */
final case class Unsubscribe(channel: String, channelsCount: Int) extends PubSubMessage

/**
 * This message is triggered after we successfully unsubscribed from a pattern.
 */
final case class PUnsubscribe(channel: String, patternsCount: Int) extends PubSubMessage

/**
 * This message is triggered when a message is received.
 */
final case class Message(channel: String, message: String) extends PubSubMessage

/**
 * This message is triggered when a message that matches a pattern is received.
 */
final case class PMessage(pattern: String, channel: String, message: String) extends PubSubMessage

/**
 * This message is triggered if an error occurs.
 */
final case class Error(exception: Throwable) extends PubSubMessage
