package scredis.commands

import scredis.AbstractClient
import scredis.protocol.requests.TransactionRequests._

import scala.concurrent.Future

/**
 * This trait implements transaction commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait TransactionCommands { self: AbstractClient =>
  
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
  
}
