package scredis

import com.typesafe.config.{ ConfigFactory, Config }

import org.apache.commons.pool.impl.GenericObjectPool

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor

/**
 * Holds all configurable parameters.
 * 
 * @see reference.conf
 */
class RedisConfig(config: Config = ConfigFactory.load().getConfig("scredis")) {
  def this(configName: String) = this(ConfigFactory.load(configName).getConfig("scredis"))
  def this(configName: String, path: String) = this(ConfigFactory.load(configName).getConfig(path))
  
  private val referenceConfig = ConfigFactory.defaultReference().getConfig("scredis")
  private val mergedConfig = config.withFallback(referenceConfig)
  mergedConfig.checkValid(referenceConfig)
  
  private def optionally[A](key: String)(f: => A)(implicit config: Config): Option[A] = {
    if (config.hasPath(key)) {
      Some(f)
    } else {
      None
    }
  }
  
  private def parseDuration(key: String)(implicit config: Config): Duration = {
    val duration = Duration.create(config.getString(key))
    if (!duration.isFinite || duration.toMillis <= 0) {
      Duration.Inf
    } else {
      duration
    }
  }
  
  private def parseFiniteDuration(key: String)(implicit config: Config): FiniteDuration = {
    val duration = parseDuration(key)
    if (duration.isFinite) {
      FiniteDuration(duration.length, duration.unit)
    } else {
      throw new IllegalArgumentException(s"$key must be finite")
    }
  }

  object Redis {
    private implicit val config = mergedConfig.getConfig("redis")
    val Host = config.getString("host")
    val Port = config.getInt("port")
    val PasswordOpt = optionally("password") {
      config.getString("password")
    }
    val Database = config.getInt("database")
    val NameOpt = optionally("name") {
      config.getString("name")
    }
  }
  
  object IO {
    private implicit val config = mergedConfig.getConfig("io")
    val ConnectTimeout = parseFiniteDuration("connect-timeout")
    val ReceiveTimeoutOpt = optionally("receive-timeout") {
      parseFiniteDuration("receive-timeout")
    }
    
    val MaxWriteBatchSize = config.getInt("max-write-batch-size")
    val TCPSendBufferSizeHint = config.getInt("tcp-send-buffer-size-hint")
    val TCPReceiveBufferSizeHint = config.getInt("tcp-receive-buffer-size-hint")
    
    object Akka {
      private implicit val config = IO.config.getConfig("akka")
      val ActorSystemName = config.getString("actor-system-name")
      val IODispatcherPath = config.getString("io-dispatcher-path")
      val ListenerDispatcherPath = config.getString("listener-dispatcher-path")
      val DecoderDispatcherPath = config.getString("decoder-dispatcher-path")
    }
  }
  
  object Global {
    private implicit val config = mergedConfig.getConfig("global")
    val MaxConcurrentRequestsOpt = optionally("max-concurrent-requests") {
      config.getInt("max-concurrent-requests")
    }
    
    object EncodeBufferPool {
      private implicit val config = Global.config.getConfig("encode-buffer-pool")
      val PoolMaxCapacity = config.getInt("pool-max-capacity")
      val BufferMaxSize = config.getInt("buffer-max-size")
    }
  }
  
  // Initialization
  Redis
  IO
  Global
  
}

object RedisConfig {
  def apply() = new RedisConfig()
  def apply(config: Config) = new RedisConfig(config)
  def apply(configName: String) = new RedisConfig(configName)
  def apply(configName: String, path: String) = new RedisConfig(configName, path)
}

object RedisConfigDefaults {
  val Config = new RedisConfig()
  val Redis = Config.Redis
  val IO = Config.IO
  val Global = Config.Global
}