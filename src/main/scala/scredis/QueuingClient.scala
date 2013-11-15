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

import org.slf4j.LoggerFactory

import scredis.util.Pattern.retry
import scredis.exceptions._
import scredis.io.{ ConnectionException, ConnectionTimeoutException }

import scala.collection.mutable.{ ListBuffer, ArrayBuffer }
import scala.concurrent.{ ExecutionContext, Promise, Future }
import scala.util.{ Try, Success, Failure }

/**
 * Defines a client that queues commands before sending them all as part of a pipeline.
 * This serves as a base class for [[scredis.PipelineClient]] and [[scredis.TransactionalClient]].
 */
abstract class QueuingClient(client: Client) {
  protected type Command = (Seq[Array[Byte]], (Char, Array[Byte]) => Any)
  
  private implicit val logger = LoggerFactory.getLogger(getClass)
  private val results = ArrayBuffer[Promise[Any]]()

  protected val DefaultCommandOptions = client.DefaultCommandOptions
  
  protected val queued = ListBuffer[Command]()
  
  protected val name: String
  protected val methodName: String

  protected def async[A](body: Client => A)(implicit opts: CommandOptions): Future[A] = {
    if (isClosed) throw RedisProtocolException("Queuing command on a closed %s", name)
    val index = queued.size
    client.startQueuing()
    body(client)
    queued ++= client.stopQueuing()
    val promise = Promise[Any]()
    synchronized { results += promise }
    promise.future.asInstanceOf[Future[A]]
  }

  protected def handleException(e: Throwable): Unit = throw e
  
  protected def completeWithException(e: Throwable): Unit = results.foreach(_.complete(Failure(e)))

  protected def runImpl(commands: List[Command]): IndexedSeq[Try[Any]]

  protected def run()(implicit opts: CommandOptions): IndexedSeq[Try[Any]] = {
    if (isClosed) throw RedisProtocolException("Calling %s on a closed %s", methodName, name)
    var results: IndexedSeq[Try[Any]] = null
    try {
      val currentTimeout = client.currentTimeout
      if (currentTimeout != opts.timeout) client.setTimeout(opts.timeout)

      results = if (opts.tries <= 1) client.reconnectOnce {
        runImpl(queued.toList)
      } else {
        retry(opts.tries, opts.sleep) { count =>
          if (count == 1) client.reconnectOnce {
            runImpl(queued.toList)
          } else try {
            runImpl(queued.toList)
          } catch {
            case e: ConnectionException => throw RedisConnectionException(e, None)
            case e: ConnectionTimeoutException => throw RedisConnectionException(e, None)
          }
        }
      }
      for(i <- 0 until results.size) this.results(i).complete(results(i))
    } catch {
      case e: Throwable => {
        completeWithException(e)
        throw e
      }
    }
    results
  }

  private[scredis] def queue[A](body: Client => A)(
    implicit opts: CommandOptions
  ): Future[A] = async(body)(opts)

  /**
   * Returns whether this `QueuingClient` has been used already
   * @return `true` if this `QueuingClient` has been used already, `false` otherwise
   */
  def isClosed: Boolean = !results.isEmpty && results.forall(_.isCompleted)

}