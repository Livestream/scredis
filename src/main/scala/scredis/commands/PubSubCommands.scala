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
package scredis.commands

import akka.util.Duration

import scredis.CommandOptions
import scredis.protocol.Protocol
import scredis.pubsub._
import scredis.exceptions.RedisProtocolException

/**
 * This trait implements pub/sub commands.
 */
trait PubSubCommands { self: Protocol =>
  private val SubscribeType = Names.Subscribe.toLowerCase
  private val PSubscribeType = Names.PSubscribe.toLowerCase
  private val UnsubscribeType = Names.Unsubscribe.toLowerCase
  private val PUnsubscribeType = Names.PUnsubscribe.toLowerCase
  private val MessageType = "message"
  private val PMessageType = "pmessage"

  private var consumer: Consumer = null
  private var subscribed = false

  private[scredis] def startConsumerIfNeeded(
    pf: PartialFunction[PubSubMessage, Any]
  ): Unit = synchronized {
    if (subscribed == false) {
      subscribed = true
      connection.setTimeout(Duration.Inf)
      consumer = new Consumer(this, pf)
      consumer.start()
    }
  }

  private[scredis] def setUnSubscribed(): Unit = synchronized {
    subscribed = false
    connection.restoreDefaultTimeout()
    consumer = null
  }

  private[scredis] class Consumer(
    producer: Protocol,
    pf: PartialFunction[PubSubMessage, Any]
  ) extends Thread {

    private val defaultErrorHandler: PartialFunction[PubSubMessage, Any] = {
      case Error(e) => throw e
    }

    private def parse(data: Iterator[String]): List[PubSubMessage] = {
      if (data.isEmpty) return List()

      val list = collection.mutable.MutableList[PubSubMessage]()
      while (data.hasNext) list += (data.next match {
        case SubscribeType => Subscribe(data.next, data.next.toInt)
        case PSubscribeType => PSubscribe(data.next, data.next.toInt)
        case UnsubscribeType => Unsubscribe(data.next, data.next.toInt)
        case PUnsubscribeType => PUnsubscribe(data.next, data.next.toInt)
        case MessageType => Message(data.next, data.next)
        case PMessageType => PMessage(data.next, data.next, data.next)
        case x => Error(RedisProtocolException("Invalid message received: %s", x))
      })
      list.toList
    }

    override def run() = {
      var continue = true
      var hasChannels = false
      var hasPatterns = false
      try {
        while (continue) {
          val iterator = receive(
            asMultiBulk[Array[Byte], Option[String], List](asAny)
          ).flatten.toIterator
          for (message <- parse(iterator)) {
            message match {
              case Subscribe(_, _) => hasChannels = true
              case PSubscribe(_, _) => hasPatterns = true
              case Unsubscribe(_, count) => {
                if (count == 0) hasChannels = false
                continue = hasChannels || hasPatterns
                if (continue) producer.synchronized { producer.notify() }
              }
              case PUnsubscribe(_, count) => {
                if (count == 0) hasPatterns = false
                continue = hasChannels || hasPatterns
                if (continue) producer.synchronized { producer.notify() }
              }
              case _ =>
            }
            if (pf.isDefinedAt(message)) pf(message)
          }
        }
      } catch {
        case e: Throwable => (pf orElse defaultErrorHandler)(Error(e))
      } finally {
        setUnSubscribed()
        producer.synchronized { producer.notify() }
      }
    }

  }

  private def subscribeRaw(channel: String, channels: String*): Unit =
    sendOnly((Names.Subscribe :: channel :: channels.toList): _*)

  private def pSubscribeRaw(channel: String, channels: String*): Unit =
    sendOnly((Names.PSubscribe :: channel :: channels.toList): _*)

  private def unsubscribeRaw(channels: String*): Unit =
    sendOnly((Names.Unsubscribe :: channels.toList): _*)

  private def pUnsubscribeRaw(patterns: String*): Unit =
    sendOnly((Names.PUnsubscribe :: patterns.toList): _*)

  /**
   * Determines whether the client is listening to some channels or patterns.
   *
   * @return true if the client is listening for some channels or patterns, false otherwise
   */
  def isSubscribed: Boolean = synchronized { subscribed }

  /**
   * Publishes a message to a channel.
   *
   * @param channel the name of the channel
   * @param message the message payload
   * @return the number of clients that received the message
   *
   * @since 2.0.0
   */
  def publish(channel: String, message: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send(Names.Publish, channel, message)(asInteger)

  /**
   * Listens for messages published to the given channels.
   *
   * @note Once the client enters the subscribed state it is not supposed to issue any other
   * commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * @param channel the name of the first channel
   * @param channels additional names of channels to listen to
   * @param pf partial function handling received messages
   *
   * @since 2.0.0
   */
  def subscribe(channel: String, channels: String*)(
    pf: PartialFunction[PubSubMessage, Any]
  ): Unit = {
    subscribeRaw(channel, channels: _*)
    startConsumerIfNeeded(pf)
  }

  /**
   * Listens for messages published to channels matching the given patterns.
   *
   * @note Once the client enters the subscribed state it is not supposed to issue any other
   * commands, except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * @param pattern the first pattern
   * @param patterns additional patterns
   * @param pf partial function handling received messages
   *
   * @since 2.0.0
   */
  def pSubscribe(pattern: String, patterns: String*)(
    pf: PartialFunction[PubSubMessage, Any]
  ): Unit = {
    pSubscribeRaw(pattern, patterns: _*)
    startConsumerIfNeeded(pf)
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
  def unsubscribe(channels: String*): Unit = synchronized {
    if (isSubscribed == false) return
    unsubscribeRaw(channels: _*)
    wait()
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
  def pUnsubscribe(patterns: String*): Unit = synchronized {
    if (isSubscribed == false) return
    pUnsubscribeRaw(patterns: _*)
    wait()
  }

}