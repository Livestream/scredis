package scredis.commands

import scredis.TransactionBuilder
import scredis.io.TransactionEnabledConnection
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
trait TransactionCommands { self: TransactionEnabledConnection =>
  
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
  
  def inTransaction(f: TransactionBuilder => Any): Future[IndexedSeq[Try[Any]]] = {
    val builder = new TransactionBuilder()
    try {
      f(builder)
      sendTransaction(builder.result())
    } catch {
      case e: Throwable => Future.failed(RedisTransactionBuilderException(cause = e))
    }
  }
  
  def withTransaction[A](f: TransactionBuilder => A): A = {
    val builder = new TransactionBuilder()
    try {
      val result = f(builder)
      sendTransaction(builder.result())
      result
    } catch {
      case e: Throwable => throw RedisTransactionBuilderException(cause = e)
    }
  }
  
}
