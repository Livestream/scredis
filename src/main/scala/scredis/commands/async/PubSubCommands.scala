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
package scredis.commands.async

import akka.dispatch.Future

import scredis.CommandOptions

/**
 * This trait implements asynchronous pub/sub commands.
 */
trait PubSubCommands extends Async {

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
  ): Future[Long] = async(_.publish(channel, message))

}