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
 * This trait implements asynchronous connection commands.
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 */
trait ConnectionCommands extends Async {

  /**
   * Authenticates to the server.
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Unit] =
    async(_.auth(password))

  /**
   * Echoes the given string on the server.
   *
   * @param message the message to echo
   * @return the echoed message
   *
   * @since 1.0.0
   */
  def echo(message: String)(implicit opts: CommandOptions = DefaultCommandOptions): Future[String] =
    async(_.echo(message))

  /**
   * Pings the server. This command is often used to test if a connection is still alive,
   * or to measure latency.
   *
   * @return PONG
   *
   * @since 1.0.0
   */
  def ping()(implicit opts: CommandOptions = DefaultCommandOptions): Future[String] =
    async(_.ping())

  /**
   * Changes the selected database on the current client.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(db: Int)(implicit opts: CommandOptions = DefaultCommandOptions): Future[Unit] =
    async(_.select(db))

}