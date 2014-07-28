package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.ConnectionRequests._

import scala.concurrent.Future

/**
 * This trait implements connection commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ConnectionCommands { self: NonBlockingConnection =>
  
  /**
   * Authenticates to the server.
   * 
   * @note Use the empty string to re-authenticate with no password.
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
  def quit(): Future[Unit] = send(Quit())

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