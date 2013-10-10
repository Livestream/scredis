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

import akka.dispatch.ExecutionContext

import scredis.commands.async._
import scredis.exceptions._

/**
 * Represents a client that queues command before pipelining them all as part of a
 * single request.
 */
final class PipelineClient private[scredis] (client: Client) extends QueuingClient(client)
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

  protected val name = "pipeline"
  protected val methodName = "sync()"

  protected def runImpl(commands: List[Command]): IndexedSeq[Either[RedisCommandException, Any]] = {
    val (args, ases) = commands.unzip
    client.sendPipeline(args)
    ases.map(client.receiveWithError).toIndexedSeq
  }

  /**
   * Executes the pipeline and returns all results.
   * 
   * @return an `IndexedSeq` containing the results of each command, in the order they were queued.
   */
  def sync()(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): IndexedSeq[Either[RedisCommandException, Any]] = run()

}