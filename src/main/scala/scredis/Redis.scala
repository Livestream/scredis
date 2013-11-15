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

import com.typesafe.config.Config

import org.slf4j.LoggerFactory

import org.apache.commons.lang3.concurrent.BasicThreadFactory

import akka.dispatch.{ Future, Promise, ExecutionContext }
import akka.util.{ Duration, FiniteDuration }

import scredis.commands.async._
import scredis.exceptions._

import scala.collection.mutable.ListBuffer

import java.util.concurrent._
import java.util.concurrent.TimeUnit._
import java.util.{ Timer, TimerTask }
import java.util.concurrent.locks.ReentrantLock

/**
 * Contains the parameters associated to a command. This class can be implicitly passed when
 * calling almost any command as to modify its behavior.
 * 
 * @param timeout maximum duration for the execution of a command, can be infinite
 * @param tries maximum number of times the command will be executed in case recoverable errors
 * occur such as timeouts
 * @param sleep duration of time to sleep between subsequent tries
 * @param force if `true` and if automatic pipelining is enabled, forces the command to be executed
 * immediately
 */
final case class CommandOptions(
  timeout: Duration = RedisConfigDefaults.Client.Timeout,
  tries: Int = RedisConfigDefaults.Client.Tries,
  sleep: Option[FiniteDuration] = RedisConfigDefaults.Client.Sleep,
  force: Boolean = false
)

/**
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define redis [[scredis.Redis]]
 * @define tc com.typesafe.Config
 */
final class Redis private[scredis] (
  protected val config: RedisConfig,
  ecOpt: Option[ExecutionContext]
) extends Async
  with ConnectionCommands
  with ServerCommands
  with KeysCommands
  with StringsCommands
  with HashesCommands
  with ListsCommands
  with SetsCommands
  with SortedSetsCommands
  with ScriptingCommands
  with PubSubCommands
  with TransactionalCommands {
  
  /**
   * Constructs a $redis instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $redis
   */
  def this(config: RedisConfig = RedisConfig()) = this(config, None)
  
  /**
   * Constructs a $redis instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Redis(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def this(config: Config) = this(RedisConfig(config), None)
  
  /**
   * Constructs a $redis instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def this(config: Config, path: String) = this(RedisConfig(config, path), None)
  
  /**
   * Constructs a $redis instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Redis(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $redis
   */
  def this(configName: String) = this(RedisConfig(configName), None)
  
  /**
   * Constructs a $redis instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def this(configName: String, path: String) = this(RedisConfig(configName, path), None)

  private val IsAutomaticPipeliningEnabled = config.Async.IsAutomaticPipeliningEnabled
  private val Threshold = config.Async.Threshold
  private val Interval = config.Async.Interval
  private val PoolNumber = newPoolNumber.toString

  private val logger = LoggerFactory.getLogger(getClass)
  
  private val timerTask = if(IsAutomaticPipeliningEnabled) Some((
    new Timer(config.Async.TimerThreadNamingPattern.replace("$p", PoolNumber)),
    new TimerTask() {
      def run(): Unit = executePipelineIfNeeded()
    }
  )) else None
  
  private val executor = ecOpt match {
    case Some(_) => None
    case None => Some(newThreadPool())
  }
  
  implicit val ec = ecOpt.getOrElse(ExecutionContext.fromExecutorService(executor.get))
  
  private val pool = ClientPool(config)

  private val lock = new ReentrantLock()
  private val commands = new ConcurrentHashMap[
    Int,
    ListBuffer[(Client => Any, CommandOptions, Promise[Any])]
  ]()

  private var index = -1
  @volatile private var shouldShutdown = false
  
  protected val DefaultCommandOptions = CommandOptions(
    config.Client.Timeout,
    config.Client.Tries,
    config.Client.Sleep
  )
  
  private def newThreadPool(): ExecutorService = {
    val threadFactory = new BasicThreadFactory.Builder()
      .namingPattern(
        config.Async.Executors.ThreadsNamingPattern.replace("$p", PoolNumber).replace("$t", "%d")
      )
      .priority(config.Async.Executors.ThreadsPriority)
      .build()
    
    new ThreadPoolExecutor(
      config.Async.Executors.Threads,
      config.Async.Executors.Threads,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](config.Async.Executors.QueueCapacity),
      threadFactory
    )
  }
  
  private def nextIndex(): Int = synchronized {
    if(index == Integer.MAX_VALUE) index = 0 else index += 1
    commands.put(index, ListBuffer())
    index
  }
  
  private def aggregateCommandOptions(options: List[CommandOptions]): CommandOptions = {
    var _timeout: Long = 0
    var _tries: Int = 0
    var _sleep: Long = 0
    for(CommandOptions(timeout, tries, sleep, _) <- options) {
      _timeout = math.max(_timeout, timeout.toMillis)
      _tries = math.max(_tries, tries)
      _sleep = math.max(_sleep, sleep.map(_.toMillis).getOrElse(0L))
    }
    CommandOptions(
      Duration.create(_timeout, MILLISECONDS),
      _tries,
      if(_sleep > 0) Some(new FiniteDuration(_sleep, MILLISECONDS)) else None
    )
  }

  private def executePipeline(index: Int): Unit = {
    val commands = this.commands.get(index)
    if (logger.isDebugEnabled) {
      logger.debug("Pipelining %d commands".format(commands.size))
    }
    try {
      withClient(client => {
        val options = ListBuffer[CommandOptions]()
        val promises = ListBuffer[Promise[Any]]()
        val pipeline = client.pipeline()
        for((body, option, promise) <- commands) {
          try {
            pipeline.queue(body)(option)
            promises += promise
            options += option
          } catch {
            case e: Throwable => {
              logger.error(
                "An error occurred while trying to queue a command as part of a pipeline", e
              )
              promise.complete(Left(e))
            }
          }
        }
        try {
          pipeline.sync()(aggregateCommandOptions(options.toList)).zip(promises).foreach {
            case (result, promise) => promise.complete(result)
          }
        } catch {
          case e: Throwable => {
            logger.error("An error occurred while trying to sync an automatic pipeline", e)
            promises.foreach(_.complete(Left(e)))
          }
        }
      })
    } catch {
      case e: Throwable => {
        logger.error("An error occurred while automatically pipelining some commands", e)
        commands.foreach {
          case (_, _, promise) => promise.complete(Left(e))
        }
      }
    }
    this.commands.remove(index)
  }

  private def executePipelineIfNeeded(): Unit = {
    if(lock.tryLock()) try {
      val executeIndex = synchronized {
        if(this.commands.isEmpty) {
          lock.unlock()
          return
        }
        val commands = this.commands.get(index)
        if(commands.size > 0) {
          val currentIndex = index
          nextIndex()
          currentIndex
        } else {
          -1
        }
      }
      if(executeIndex >= 0) Future { executePipeline(executeIndex) }
    } catch {
      case e: Throwable => logger.error("An unexpected error occurred", e)
    } finally {
      try {
        lock.unlock()
      } catch {
        case e: Throwable =>
      }
    }
  }

  protected def async[A](body: Client => A)(implicit opts: CommandOptions): Future[A] = {
    if(shouldShutdown) throw RedisProtocolException(
      "Trying to queue a command after having called quit()"
    )
    if (IsAutomaticPipeliningEnabled && opts.force == false) {
      val (future, executeIndex) = synchronized {
        val commands = this.commands.get(index)
        val future = try {
          val promise = Promise[Any]()
          commands += ((body, opts, promise))
          promise.future.asInstanceOf[Future[A]]
        } catch {
          case e: Throwable => {
            logger.error("An error occurred while automatically queuing a command", e)
            throw e
          }
        }
        val executeIndex = if (Threshold.isDefined && commands.size >= Threshold.get) {
          val currentIndex = index
          nextIndex()
          currentIndex
        } else {
          -1
        }
        (future, executeIndex)
      }
      if(executeIndex >= 0) Future { executePipeline(executeIndex) }
      future
    } else {
      withClientAsync(body)
    }
  }
  
  private[scredis] def selectInternal(db: Int)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = synchronized {
    flushAutomaticPipeline()
    pool.select(db)
  }
  
  private[scredis] def authInternal(passwordOpt: Option[String])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = synchronized {
    flushAutomaticPipeline()
    pool.auth(passwordOpt)
  }
  
  private[scredis] def closeInternal(): Unit = synchronized {
    shouldShutdown = true
    flushAutomaticPipeline()
    while(this.commands.size > 1) Thread.sleep(100)
    
    timerTask.foreach {
      case (timer, task) => {
        task.cancel()
        timer.cancel()
      }
    }
    
    try {
      pool.close()
    } catch {
      case e: Throwable =>
    }
    try {
      executor.foreach(_.shutdown())
    } catch {
      case e: Throwable =>
    }
  }

  def borrowClient(): Client = pool.borrowClient()
  def returnClient(client: Client): Unit = pool.returnClient(client)
  def withClient[A](body: Client => A): A = pool.withClient(body)
  def withClientAsync[A](body: Client => A): Future[A] = Future {
    withClient(body)
  }
  
  def sync[A](body: Client => A): A = withClient(body)

  def flushAutomaticPipeline(): Unit = {
    if(IsAutomaticPipeliningEnabled) executePipelineIfNeeded()
  }
  
  /**
   * Changes the selected database on all underlying clients.
   * 
   * @note if the provided database index is invalid then previously set index remains.
   * 
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  override def select(
    db: Int
  )(implicit opts: CommandOptions = DefaultCommandOptions): Future[Unit] = Future {
    selectInternal(db)
  }
  
  /**
   * Authenticates/Deauthenticates all clients to/from the server.
   * 
   * @note If the provided password is incorrect, then whatever was set as the password before
   * remains. You must provide the empty string if you want to deauthenticate previously
   * authenticated clients.
   *
   * @param password the server password, or the empty string if the server does not require any
   * @throws $e if the password is incorrect
   *
   * @since 1.0.0
   */
  override def auth(password: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[Unit] = Future {
    authInternal(if(password.isEmpty) None else Some(password))
  }
  
  /**
   * Changes the selected database on all underlying clients.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  def selectSync(
    db: Int
  )(implicit opts: CommandOptions = DefaultCommandOptions): Unit = selectInternal(db)
  
  /**
   * Closes all connections, including internal pool and shut down the default execution context.
   * 
   * @note If you provided your own execution context using `withExecutionContext()`, it will not
   * be shutdown by this method. It is your responsibility to do so.
   *
   * @since 1.0.0
   */
  def quit(): Unit = closeInternal()
  
  if(IsAutomaticPipeliningEnabled) nextIndex()
  
  timerTask.foreach {
    case (timer, task) => timer.scheduleAtFixedRate(task, Interval.toMillis, Interval.toMillis)
  }

}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define redis [[scredis.Redis]]
 * @define tc com.typesafe.Config
 */
object Redis {
  
  /**
   * Constructs a $redis instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $redis
   */
  def apply(config: RedisConfig = RedisConfig()): Redis = new Redis(config)
  
  /**
   * Constructs a $redis instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Redis(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def apply(config: Config): Redis = new Redis(config)
  
  /**
   * Constructs a $redis instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def apply(config: Config, path: String): Redis = new Redis(config, path)
  
  /**
   * Constructs a $redis instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Redis(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $redis
   */
  def apply(configName: String): Redis = new Redis(configName)
  
  /**
   * Constructs a $redis instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def apply(configName: String, path: String): Redis = new Redis(configName, path)

  /**
   * Constructs a $redis instance from the default loaded config using the provided execution
   * context
   * 
   * @note It is your responsibility to shutdown the execution context as [[scredis.Redis.quit]]
   * will not do it.
   * 
   * @return the constructed $redis
   */
  def withExecutionContext()(implicit ec: ExecutionContext): Redis =
    new Redis(RedisConfig(), Some(ec))
  
  /**
   * Constructs a $redis instance from a [[scredis.RedisConfig]] using the provided execution
   * context
   * 
   * @note It is your responsibility to shutdown the execution context as [[scredis.Redis.quit]]
   * will not do it.
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $redis
   */
  def withExecutionContext(config: RedisConfig)(implicit ec: ExecutionContext): Redis =
    new Redis(config, Some(ec))
  
  /**
   * Constructs a $redis instance from a $tc using the provided execution context
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Redis.withExecutionContext(config, "scredis")
   * }}}
   * It is your responsibility to shutdown the execution context as [[scredis.Redis.quit]]
   * will not do it.
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def withExecutionContext(config: Config)(
    implicit ec: ExecutionContext
  ): Redis = new Redis(RedisConfig(config), Some(ec))
  
  /**
   * Constructs a $redis instance from a $tc using the provided path and execution context
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis. Also, it is your
   * responsibility to shutdown the execution context as [[scredis.Redis.quit]] will not do it.
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def withExecutionContext(config: Config, path: String)(
    implicit ec: ExecutionContext
  ): Redis = new Redis(RedisConfig(config, path), Some(ec))
  
  /**
   * Constructs a $redis instance from a config file using the provided execution context
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Redis.withExecutionContext(configName, "scredis")
   * }}}
   * It is your responsibility to shutdown the execution context as [[scredis.Redis.quit]]
   * will not do it.
   * 
   * @param configName config filename
   * @return the constructed $redis
   */
  def withExecutionContext(configName: String)(implicit ec: ExecutionContext): Redis =
    new Redis(RedisConfig(configName), Some(ec))
  
  /**
   * Constructs a $redis instance from a config file and using the provided path and execution
   * context
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis. Also, it is your
   * responsibility to shutdown the execution context as [[scredis.Redis.quit]] will not do it.
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def withExecutionContext(configName: String, path: String)(implicit ec: ExecutionContext): Redis =
    new Redis(RedisConfig(configName, path), Some(ec))
  
}

