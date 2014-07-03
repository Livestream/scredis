package scredis

import com.typesafe.config.Config

import akka.actor.ActorSystem

import scredis.protocol.Protocol
import scredis.commands._
import scredis.exceptions._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Defines a basic Redis client supporting all commands.
 * 
 * @note a Client is '''not''' thread-safe. To execute commands within multiple threads, use
 * [[scredis.Redis]] or [[scredis.ClientPool]].
 * 
 * @param host server address
 * @param port server port
 * @param password server password
 * @param database database index to select
 * @param timeout maximum duration for the execution of a command, can be infinite
 * 
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define client [[scredis.Client]]
 * @define tc com.typesafe.Config
 */
final class Client(
  private var host: String = RedisConfigDefaults.Client.Host,
  private var port: Int = RedisConfigDefaults.Client.Port,
  private var passwordOpt: Option[String] = RedisConfigDefaults.Client.Password,
  private var database: Int = RedisConfigDefaults.Client.Database,
  timeout: Duration = RedisConfigDefaults.Client.Timeout
)(implicit system: ActorSystem) extends AbstractClient(system, host, port)
  with ConnectionCommands
  with ServerCommands
  with KeyCommands
  with StringCommands
  with HashCommands
  with ListCommands
  with SetCommands
  with SortedSetCommands
  with ScriptingCommands
  with PubSubCommands
  with TransactionCommands {
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def this(config: RedisConfig)(implicit system: ActorSystem) = this(
    config.Client.Host,
    config.Client.Port,
    config.Client.Password,
    config.Client.Database,
    config.Client.Timeout
  )
  
  /**
   * Constructs a $client instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Client(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $client
   */
  def this(config: Config)(implicit system: ActorSystem) = this(RedisConfig(config))
  
  /**
   * Constructs a $client instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def this(config: Config, path: String)(implicit system: ActorSystem) = this(
    RedisConfig(config, path)
  )
  
  /**
   * Constructs a $client instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * new Client(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $client
   */
  def this(configName: String)(implicit system: ActorSystem) = this(RedisConfig(configName))
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def this(configName: String, path: String)(implicit system: ActorSystem) = this(
    RedisConfig(configName, path)
  )
  
  private def authAndSelect(): Future[Unit] = {
    passwordOpt.map(auth).getOrElse {
      Future.successful(())
    }.flatMap { _ =>
      if (database > 0) {
        select(database)
      } else {
        Future.successful(())
      }
    }
  }
  
  /**
   * Authenticates to the server.
   *
   * @param password the server password
   * @throws $e if authentication failed
   *
   * @since 1.0.0
   */
  override def auth(password: String): Future[Unit] = {
    this.passwordOpt = if (password.isEmpty) {
      None
    } else {
      Some(password)
    }
    super.auth(password)
  }
  
  /**
   * Changes the selected database on the current client.
   *
   * @param db database index
   * @throws $e if the database index is invalid
   *
   * @since 1.0.0
   */
  override def select(database: Int): Future[Unit] = {
    this.database = database
    super.select(database)
  }
  
  /*
  try {
    connection.connect()
  } catch {
    case e: Throwable =>
  }*/
  
}

/**
 * The companion object provides additional friendly constructors.
 * 
 * @define client [[scredis.Client]]
 * @define tc com.typesafe.Config
 */
object Client {

  /**
   * Creates a $client
   * 
   * @param host server address
   * @param port server port
   * @param password server password
   * @param database database index to select
   * @param timeout maximum duration for the execution of a command, can be infinite
   */
  def apply(
    host: String = RedisConfigDefaults.Client.Host,
    port: Int = RedisConfigDefaults.Client.Port,
    password: Option[String] = RedisConfigDefaults.Client.Password,
    database: Int = RedisConfigDefaults.Client.Database,
    timeout: Duration = RedisConfigDefaults.Client.Timeout
  )(implicit system: ActorSystem): Client = new Client(host, port, password, database, timeout)
  
  
  /**
   * Constructs a $client instance from a [[scredis.RedisConfig]]
   * 
   * @param config [[scredis.RedisConfig]]
   * @return the constructed $client
   */
  def apply(config: RedisConfig)(implicit system: ActorSystem): Client = new Client(config)
  
  /**
   * Constructs a $client instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Client(config, "scredis")
   * }}}
   * 
   * @param config $tc
   * @return the constructed $client
   */
  def apply(config: Config)(implicit system: ActorSystem): Client = new Client(config)
  
  /**
   * Constructs a $client instance from a $tc using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param config $tc
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(config: Config, path: String)(implicit system: ActorSystem): Client = new Client(
    config, path
  )
  
  /**
   * Constructs a $client instance from a config file.
   * 
   * @note The config file must contain the scredis object at its root.
   * This constructor is equivalent to {{{
   * Client(configName, "scredis")
   * }}}
   * 
   * @param configName config filename
   * @return the constructed $client
   */
  def apply(configName: String)(implicit system: ActorSystem): Client = new Client(configName)
  
  /**
   * Constructs a $client instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $client
   */
  def apply(configName: String, path: String)(implicit system: ActorSystem): Client = new Client(
    configName, path
  )

}