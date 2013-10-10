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
package scredis

import scredis.commands.async._
import scredis.parsing.LongParser
import scredis.exceptions._

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * The `TransactionalClient` represents a `Redis` transaction.
 */
final class TransactionalClient private[scredis] (client: Client) extends QueuingClient(client)
  with ConnectionCommands
  with ServerCommands
  with KeysCommands
  with StringsCommands
  with HashesCommands
  with ListsCommands
  with SetsCommands
  with SortedSetsCommands
  with ScriptingCommands
  with PubSubCommands {

  protected val name = "transaction"
  protected val methodName = "exec()"

  protected override def handleException(e: Throwable): Unit =
    throw RedisTransactionException("Transaction has been discarded: %s".format(e))

  protected def runImpl(commands: List[Command]): IndexedSeq[Try[Any]] = {
    val (args, ases) = commands.unzip
    client.sendPipeline(args)
    try {
      for (i <- 1 to commands.size) client.receive(client.asStatus)
    } catch {
      case e: Throwable => {
        client.send("DISCARD")(client.asStatus)
        throw e
      }
    }
    val discarded = client.send("EXEC")((c: Char, b: Array[Byte]) => LongParser.parse(b) < 0)
    // Transaction was aborted due to some watched keys
    if (discarded) throw RedisTransactionException(
      "Transaction has been discarded due to some watched keys"
    )
    ases.map(client.receiveWithError).toIndexedSeq
  }

  /**
   * Discards the transaction and completes all futures with a
   * [[scredis.exceptions.RedisTransactionException]].
   */
  def discard(): Unit = {
    if (isClosed) throw RedisProtocolException("Calling discard() on a closed transaction")
    client.send("DISCARD")(client.asStatus)
    completeWithException(RedisTransactionException("Transaction has been explicitely discarded"))
  }

  /**
   * Executes the transaction and returns all results.
   * 
   * @return an `IndexedSeq` containing the results of each command, in the order they were queued.
   */
  def exec()(implicit opts: CommandOptions = DefaultCommandOptions): IndexedSeq[Try[Any]] = run()

}