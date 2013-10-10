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
import akka.util.duration._

import scala.collection.mutable.ArrayBuilder
import java.net.{ Socket, SocketException, SocketTimeoutException, InetSocketAddress }
import java.io.{ BufferedInputStream, BufferedOutputStream }

/**
 * Implements the [[scredis.io.Connection]] trait using a Java Socket.
 */
final class JavaSocketConnection private[scredis] (
  val host: String,
  val port: Int,
  val timeout: Duration = 2 seconds,
  onConnect: Option[() => Any] = None,
  onDisconnect: Option[() => Any] = None
) extends Connection {

  private val CrByte = 13
  private val LfByte = 10
  private val CrByteAsByte = CrByte.toByte

  private var socket: Socket = null
  private var in: BufferedInputStream = null
  private var out: BufferedOutputStream = null
  
  private var _currentTimeout: Duration = timeout
  
  private def getSoSocketTimeout: Int =
    if(_currentTimeout.isFinite) _currentTimeout.toMillis.toInt else 0

  private def clear(): Unit = {
    socket = null
    in = null
    out = null
  }

  private def handleException(e: Throwable): Nothing = {
    val exception = e match {
      case e: ConnectionException => e
      case e: SocketTimeoutException => new ConnectionTimeoutException(e)
      case e: SocketException => e.getMessage match {
        case "Connection reset" => new ConnectionException(e, retry = true)
        case "Broken pipe" => new ConnectionException(e, retry = true)
        case _ => new ConnectionException(e)
      }
      case e => new ConnectionException(e)
    }
    try {
      disconnect()
    } catch {
      case e: Throwable =>
    }
    throw exception
  }

  def isConnected = (
    socket != null &&
    socket.isBound &&
    !socket.isClosed &&
    socket.isConnected &&
    !socket.isInputShutdown &&
    !socket.isOutputShutdown
  )

  def connect(): Unit = try {
    socket = new Socket()
    socket.setReuseAddress(true)
    socket.setSoLinger(true, 0)
    socket.setSoTimeout(getSoSocketTimeout)
    socket.setKeepAlive(true)
    socket.setTcpNoDelay(true)
    socket.connect(new InetSocketAddress(host, port))

    in = new BufferedInputStream(socket.getInputStream)
    out = new BufferedOutputStream(socket.getOutputStream)
    onConnect.foreach(_())
  } catch {
    case e: Throwable => {
      try { disconnect() } catch { case e: Throwable => }
      throw ConnectionException(e, Some("Could not connect to %s:%d".format(host, port)))
    }
  }

  def disconnect(): Unit = try {
    try { out.flush() } catch { case e: Throwable => }
    if(socket != null) socket.close()
  } catch {
    case e: Throwable => throw ConnectionException(e)
  } finally {
    clear()
    onDisconnect.foreach(_())
  }
  
  def reconnect(): Unit = {
    try { disconnect() } catch { case e: Throwable => e.printStackTrace() }
    connect()
  }

  def read(count: Int, consumeEndLine: Boolean = true): Array[Byte] = {
    if (!isConnected) connect()
    try {
      val buffer = new Array[Byte](count)
      var index = 0
      while (index < count) {
        val read = in.read(buffer, index, count - index)
        if (read < 0) throw ConnectionException(
          new SocketException("The server has closed the connection"),
          retry = true
        )
        index += read
      }
      if (consumeEndLine) readLine()
      buffer
    } catch {
      case e: Throwable => handleException(e)
    }
  }

  def readLine(): Array[Byte] = {
    if (!isConnected) connect()
    try {
      val buffer = new ArrayBuilder.ofByte
      var crFound = false
      while (true) {
        val byte = in.read
        if (byte < 0) throw ConnectionException(
          new SocketException("The server has closed the connection"),
          retry = true
        )
        if (!crFound) {
          if (byte == CrByte) crFound = true
          else buffer += byte.toByte
        } else {
          if (byte == LfByte) return buffer.result
          else {
            buffer += CrByteAsByte
            buffer += byte.toByte
            crFound = false
          }
        }
      }
      throw ConnectionException(new IllegalStateException())
    } catch {
      case e: Throwable => handleException(e)
    }
  }

  def write(data: Array[Byte]): Unit = {
    if (!isConnected) connect()
    try {
      out.write(data)
      out.flush()
    } catch {
      case e: SocketException => handleException(ConnectionException(e, retry = true))
      case e: Throwable => handleException(e)
    }
  }
  
  def currentTimeout: Duration = _currentTimeout

  def setTimeout(timeout: Duration): Unit = {
    if(!isConnected) connect()
    _currentTimeout = timeout
    socket.setSoTimeout(getSoSocketTimeout)
  }

  def restoreDefaultTimeout(): Unit = setTimeout(timeout)

}