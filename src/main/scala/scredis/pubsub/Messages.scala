/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
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
