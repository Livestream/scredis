package scredis

import com.typesafe.config.Config

import akka.actor._
import akka.routing._

import scredis.io.AkkaIORouterConnection
import scredis.commands._

import scala.concurrent.duration._

/**
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define redis [[scredis.Redis]]
 * @define tc com.typesafe.Config
 */
final class Redis(protected val config: RedisConfig) extends AkkaIORouterConnection(
  ActorSystem(),
  host = config.Client.Host,
  port = config.Client.Port,
  passwordOpt = config.Client.Password,
  database = config.Client.Database,
  decodersCount = 2,
  receiveTimeout = 5 seconds
) with ConnectionCommands {
  
  /**
   * Constructs a $redis instance using the default config
   * 
   * @return the constructed $redis
   */
  def this() = this(RedisConfig())
  
  /**
   * Constructs a $redis instance from a $tc
   * 
   * @note The config must contain the scredis object at its root.
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def this(config: Config) = this(RedisConfig(config))
  
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
  def this(configName: String) = this(RedisConfig(configName))
  
  /**
   * Constructs a $redis instance from a config file and using the provided path.
   * 
   * @note The path must include to the scredis object, e.g. x.y.scredis
   * 
   * @param configName config filename
   * @param path path pointing to the scredis config object
   * @return the constructed $redis
   */
  def this(configName: String, path: String) = this(RedisConfig(configName, path))

  // TODO: select & auth BROADCAST

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
   * 
   * @param config $tc
   * @return the constructed $redis
   */
  def apply(config: Config): Redis = new Redis(config)
  
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
  
}
