package scredis.commands

import scredis.TransactionBuilder
import scredis.io.{ Connection, NonBlockingConnection, TransactionEnabledConnection }
import scredis.protocol.requests.TransactionRequests._
import scredis.exceptions.RedisTransactionBuilderException

import scala.util.Try
import scala.concurrent.Future
import scredis.TransactionBuilder

/**
 * This trait implements transaction commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait TransactionCommands {
  self: Connection with NonBlockingConnection with TransactionEnabledConnection =>
  
  /**
   * Watches the given keys, which upon modification, will abort a transaction.
   *
   * @param keys keys to watch
   *
   * @since 2.2.0
   */
  def watch(keys: String*): Future[Unit] = send(Watch(keys: _*))
  
  /**
   * Forgets about all watched keys.
   *
   * @since 2.2.0
   */
  def unwatch(): Future[Unit] = send(Unwatch())
  
  /**
   * Executes a transaction and returns the results of all queued commands.
   * 
   * @param build the transaction block
   * @return vector containing the results of all queued commands
   *
   * @since 1.2.0
   */
  def inTransaction(build: TransactionBuilder => Any): Future[Vector[Try[Any]]] = {
    val builder = new TransactionBuilder()
    try {
      build(builder)
      send(builder.result())
    } catch {
      case e: Throwable => Future.failed(RedisTransactionBuilderException(cause = e))
    }
  }
  
  /**
   * Executes a transaction and returns whatever the transaction block returns.
   * 
   * @param build the transaction block
   * @return whatever 'build' returns
   *
   * @since 1.2.0
   */
  def withTransaction[A](build: TransactionBuilder => A): A = {
    val builder = new TransactionBuilder()
    try {
      val result = build(builder)
      send(builder.result())
      result
    } catch {
      case e: Throwable => throw RedisTransactionBuilderException(cause = e)
    }
  }
  
}
