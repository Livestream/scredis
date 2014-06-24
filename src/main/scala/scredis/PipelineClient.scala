package scredis

import scredis.commands.async._
import scredis.exceptions._

import scala.concurrent.ExecutionContext
import scala.util.Try

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

  protected def runImpl(commands: List[Command]): IndexedSeq[Try[Any]] = {
    val (args, ases) = commands.unzip
    client.sendPipeline(args)
    ases.map(client.receiveWithError).toIndexedSeq
  }
  
  /**
   * Executes the pipeline and returns all results.
   * 
   * @return an `IndexedSeq` containing the results of each command, in the order they were queued.
   */
  def sync()(implicit opts: CommandOptions = DefaultCommandOptions): IndexedSeq[Try[Any]] = run()

}