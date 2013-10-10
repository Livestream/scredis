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

import scredis.CommandOptions
import scredis.protocol.Protocol
import scredis.parsing.Implicits._

/**
 * This trait implements connection commands.
 *
 * @define e [[scredis.exceptions.RedisCommandException]]
 */
trait ConnectionCommands { self: Protocol =>
  import Names._

  /**
   * Authenticates to the server.
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  def auth(password: String)(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Auth, password)(asUnit)

  /**
   * Echoes the given string on the server.
   *
   * @param message	the message to echo
   * @return the echoed message
   *
   * @since 1.0.0
   */
  def echo(message: String)(implicit opts: CommandOptions = DefaultCommandOptions): String =
    send(Echo, message)(asBulk[String, String](flatten))

  /**
   * Pings the server. This command is often used to test if a connection is still alive,
   * or to measure latency.
   *
   * @return PONG
   *
   * @since 1.0.0
   */
  def ping()(implicit opts: CommandOptions = DefaultCommandOptions): String = send(Ping)(asStatus)

  /**
   * Closes the connection.
   *
   * @since 1.0.0
   */
  def quit()(implicit opts: CommandOptions = DefaultCommandOptions): Unit = {
    try {
      send(Quit)(asUnit)
    } catch {
      case e: Throwable =>
    }
    try {
      connection.disconnect()
    } catch {
      case e: Throwable =>
    }
  }

  /**
   * Changes the selected database on the current client.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def select(db: Int)(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Select, db)(asOkStatus)

}