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

import scredis.protocol.Protocol
import scredis.protocol.commands.ConnectionCommands._

import scala.concurrent.Future

/**
 * This trait implements connection commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 */
trait ConnectionCommands { self: Protocol =>

  /**
   * Authenticates to the server.
   * 
   * @note use the empty string to re-authenticate with no password
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String): Future[Unit] = send(Auth(password))

  /**
   * Echoes the given string on the server.
   *
   * @param message	the message to echo
   * @return the echoed message
   *
   * @since 1.0.0
   */
  def echo(message: String): Future[String] = send(Echo(message))

  /**
   * Pings the server. This command is often used to test if a connection is still alive,
   * or to measure latency.
   *
   * @return PONG
   *
   * @since 1.0.0
   */
  def ping(): Future[String] = send(Ping())

  /**
   * Closes the connection.
   *
   * @since 1.0.0
   */
  def quit(): Future[Unit] = send(Quit()).map {
    _ => // TODO: shutdown IO
  }

  /**
   * Changes the selected database on the current connection.
   *
   * @param database database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(database: Int): Future[Unit] = send(Select(database))
  
}