package scredis.commands

import scredis.io.Connection
import scredis.protocol.Decoder
import scredis.protocol.requests.ScriptingRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements scripting commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define p [[scredis.exceptions.RedisProtocolException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait ScriptingCommands { self: Connection =>
  
  /**
   * Executes a Lua script that does not require any keys or arguments.
   *
   * @param script the LUA script
   * @param keys keys to be used in the script
   * @param args arguments to be used in the script
   * @throws $e if an error occurs while running the script
   * @throws $p if the result could not be decoded by provided `Decoder`
   * 
   * @since 2.6.0
   */
  def eval[R: Decoder, W1: Writer, W2: Writer](
    script: String, keys: Seq[W1] = Nil, args: Seq[W2] = Nil
  ): Future[R] = send(Eval(script, keys, args))

  /**
   * Executes a cached Lua script that does not require any keys or arguments by its SHA1 digest.
   *
   * @param sha1 the SHA1 digest
   * @param keys keys to be used in the script
   * @param args arguments to be used in the script
   * @throws $e if there is no script corresponding to the provided SHA1 digest or if an error
   * occurs while running the script
   * @throws $p if the result could not be decoded by provided `Decoder`
   *
   * @since 2.6.0
   */
  def evalSHA[R: Decoder, W1: Writer, W2: Writer](
    sha1: String, keys: Seq[W1] = Nil, args: Seq[W2] = Nil
  ): Future[R] = send(EvalSHA(sha1, keys, args))
  
  /**
   * Checks existence of scripts in the script cache.
   *
   * @param sha1s SHA1 digest(s) to check for existence
   * @return SHA1 -> Boolean `Map` where $true means the script associated to the sha1 exists
   * in the cache
   *
   * @since 2.6.0
   */
  def scriptExists(sha1s: String*): Future[Map[String, Boolean]] = send(
    ScriptExists(sha1s: _*)
  )
  
  /**
   * Removes all the scripts from the script cache.
   *
   * @since 2.6.0
   */
  def scriptFlush(): Future[Unit] = send(ScriptFlush())
  
  /**
   * Kills the currently executing Lua script, assuming no write operation was yet performed by
   * the script.
   *
   * @note If the script already performed write operations it can not be killed in this way
   * because it would violate Lua script atomicity contract. In such a case only SHUTDOWN NOSAVE
   * is able to kill the script, killing the Redis process in an hard way preventing it to persist
   * with half-written information.
   *
   * @since 2.6.0
   */
  def scriptKill(): Future[Unit] = send(ScriptKill())
  
  /**
   * Loads or stores the specified Lua script into the script cache.
   *
   * @note The script is guaranteed to stay in the script cache forever (unless SCRIPT FLUSH
   * is called).
   *
   * @param script the script to be loaded into the cache
   * @return the SHA1 digest of the stored script
   * @throws $e if a compilation error occurs
   *
   * @since 2.6.0
   */
  def scriptLoad(script: String): Future[String] = send(ScriptLoad(script))
  
}