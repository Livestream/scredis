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
import scredis.protocol.{ Protocol, As }
import scredis.parsing.Implicits._

/**
 * This trait implements scripting commands.
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define p [[scredis.exceptions.RedisProtocolException]]
 */
trait ScriptingCommands { self: Protocol =>
  import Names._
  
  protected val as = new As(this)

  /**
   * Executes a Lua script that does not require any keys or arguments.
   *
   * @param script set key
   * @param as result handler
   * @throws $e if an error occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def eval[A](script: String)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(Eval, script, 0)(as(this.as))

  /**
   * Executes a Lua script with keys parameter.
   *
   * @param script set key
   * @param keys keys to be used in the script
   * @param as result handler
   * @throws $e if an error occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalWithKeys[A](script: String)(keys: String*)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(Eval :: script :: keys.size :: keys.toList: _*)(as(this.as))

  /**
   * Executes a Lua script with arguments.
   *
   * @param script set key
   * @param args arguments to be used in the script
   * @param as result handler
   * @throws $e if an error occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalWithArgs[A](script: String)(args: Any*)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(Eval :: script :: 0 :: args.toList: _*)(as(this.as))

  /**
   * Executes a Lua script with keys and arguments.
   *
   * @param script set key
   * @param keys keys to be used in the script
   * @param args arguments to be used in the script
   * @param as result handler
   * @throws $e if an error occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalWithKeysAndArgs[A](script: String)(keys: String*)(args: Any*)(
    as: As => (Char, Array[Byte]) => A
  )(implicit opts: CommandOptions = DefaultCommandOptions): A = send(
    Eval :: script :: keys.size :: keys.toList ::: args.toList: _*
  )(as(this.as))

  /**
   * Executes a cached Lua script that does not require any keys or arguments by its SHA1 digest.
   *
   * @param sha1 the SHA1 digest
   * @param as result handler
   * @throws $e if there is no script corresponding to the provided SHA1 digest or if an error
   * occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalSha[A](sha1: String)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(EvalSha, sha1, 0)(as(this.as))

  /**
   * Executes a cached Lua script with keys parameter by its SHA1 digest.
   *
   * @param sha1 the SHA1 digest
   * @param keys keys to be used in the script
   * @param as result handler
   * @throws $e if there is no script corresponding to the provided SHA1 digest or if an error
   * occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalShaWithKeys[A](sha1: String)(keys: String*)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(EvalSha :: sha1 :: keys.size :: keys.toList: _*)(as(this.as))

  /**
   * Executes a cached Lua script with arguments by its SHA1 digest.
   *
   * @param sha1 the SHA1 digest
   * @param args arguments to be used in the script
   * @param as result handler
   * @throws $e if there is no script corresponding to the provided SHA1 digest or if an error
   * occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalShaWithArgs[A](sha1: String)(args: Any*)(as: As => (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = send(EvalSha :: sha1 :: 0 :: args.toList: _*)(as(this.as))

  /**
   * Executes a cached Lua script with keys and arguments by its SHA1 digest.
   *
   * @param sha1 the SHA1 digest
   * @param keys keys to be used in the script
   * @param args arguments to be used in the script
   * @param as result handler
   * @throws $e if there is no script corresponding to the provided SHA1 digest or if an error
   * occurs while running the script
   * @throws $p if the value returned by the script does not match with the result handler
   *
   * @since 2.6.0
   */
  def evalShaWithKeysAndArgs[A](sha1: String)(keys: String*)(args: Any*)(
    as: As => (Char, Array[Byte]) => A
  )(implicit opts: CommandOptions = DefaultCommandOptions): A = send(
    EvalSha :: sha1 :: keys.size :: keys.toList ::: args.toList: _*
  )(as(this.as))

  /**
   * Checks existence of scripts in the script cache.
   *
   * @param sha1 the SHA1 digest to check for
   * @param sha1s additional digests to check for
   * @return indexed sequence of booleans where true means the script is in the cache
   *
   * @since 2.6.0
   */
  def scriptExists(sha1: String, sha1s: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): IndexedSeq[Boolean] = send(Script :: ScriptExists :: sha1 :: sha1s.toList: _*)(
    asMultiBulk[Long, Boolean, IndexedSeq](asInteger(x => x > 0))
  )

  /**
   * Removes all the scripts from the script cache.
   *
   * @since 2.6.0
   */
  def scriptFlush()(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Script, ScriptFlush)(asUnit)

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
  def scriptKill()(implicit opts: CommandOptions = DefaultCommandOptions): Unit =
    send(Script, ScriptKill)(asUnit)

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
  def scriptLoad(script: String)(implicit opts: CommandOptions = DefaultCommandOptions): String =
    send(Script, ScriptLoad, script)(asBulk[String, String](flatten))

}