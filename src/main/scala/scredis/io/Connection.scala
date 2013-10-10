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
package scredis.io

import akka.util.Duration

/**
 * Represents a connection to a `Redis` server.
 */
trait Connection {
  val timeout: Duration
  
  /**
   * Returns true if the connection is connected to a `Redis` server, false otherwise
   * @return true if the connection is connected to a `Redis` server, false otherwise
   */
  def isConnected: Boolean

  /**
   * Connects to the `Redis` server with provided host and port.
   *
   * @throws ConnectionException if unable to connect
   */
  def connect(): Unit

  /**
   * Disconnects from the `Redis` server.
   *
   * @throws ConnectionException if an error occurs while disconnecting
   */
  def disconnect(): Unit

  /**
   * Reads `count` bytes from the input stream and optionally consumes a
   * CRLF (without returning it).
   *
   * @param count the number of bytes to read
   * @param consumeEndLine  when true, reads a CRLF after having read `count` bytes
   * @return the read bytes
   * @throws ConnectionTimeoutException if a timeout occurs while trying to read
   * @throws ConnectionException if any other error occurs
   */
  def read(count: Int, consumeEndLine: Boolean = true): Array[Byte]

  /**
   * Reads the next line from the input stream (delimited by a CRLF) without
   * returning the end line delimiter
   *
   * @return the read bytes
   * @throws ConnectionTimeoutException if a timeout occurs while trying to read
   * @throws ConnectionException if any other error occurs
   */
  def readLine(): Array[Byte]

  /**
   * Writes `data` to the output stream.
   *
   * @throws ConnectionTimeoutException if a timeout occurs while trying to write
   * @throws ConnectionException if any other error occurs
   */
  def write(data: Array[Byte]): Unit

  /**
   * Returns the currently set read/write timeout of this connection.
   *
   * return the current timeout, can be infinite
   */
  def currentTimeout: Duration
  
  /**
   * Sets the read/write timeout of this connection.
   *
   * @param timeout the timeout's duration, can be infinite
   */
  def setTimeout(timeout: Duration): Unit

  /**
   * Restores the connection timeout value to the original configured value
   */
  def restoreDefaultTimeout(): Unit

  /**
   * Diconnects and reconnects
   *
   * @throws ConnectionException if an error occurs
   */
  def reconnect(): Unit
}

case class ConnectionException(
  exception: Throwable,
  message: Option[String] = None,
  retry: Boolean = false
) extends Exception(message.getOrElse(exception.toString), exception)
case class ConnectionTimeoutException(exception: Throwable) extends Exception(exception)
