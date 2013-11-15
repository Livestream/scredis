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
  def this(config: Config, path: String) = this(ConfigFactory.load().getConfig(path))
  def this(configName: String) = this(ConfigFactory.load(configName).getConfig("scredis"))
  def this(configName: String, path: String) = this(ConfigFactory.load(configName).getConfig(path))
  
  private val referenceConfig = ConfigFactory.defaultReference().getConfig("scredis")
  private val mergedConfig = config.withFallback(referenceConfig)
  mergedConfig.checkValid(referenceConfig)
  
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val client = mergedConfig.getConfig("client")
  private val pool = mergedConfig.getConfig("pool")
  private val poolEviction = pool.getConfig("eviction")
  private val async = mergedConfig.getConfig("async")
  private val asyncExecutors = async.getConfig("executors")

  private def parseDuration(value: String): Duration = {
    val duration = Duration.create(value)
    if (!duration.isFinite || duration.toMillis <= 0) Duration.Inf else duration
  }
  
  private def parseFiniteDuration(key: String, value: String): FiniteDuration = {
    val duration = parseDuration(value)
    if(duration.isFinite) FiniteDuration(duration.length, duration.unit)
    else throw new IllegalArgumentException(s"$key must be finite")
  }

  object Client {
    val Host = client.getString("host")
    val Port = client.getInt("port")
    val Password = client.hasPath("password") match {
      case true => Some(client.getString("password"))
      case false => None
    }
    val Database = client.getInt("database")
    val Timeout = client.hasPath("timeout") match {
      case true => parseDuration(client.getString("timeout"))
      case false => Duration.Inf
    }
    val Tries = client.getInt("tries") match {
      case x if x >= 1 => x
      case _ => 1
    }
    val Sleep = client.hasPath("sleep") match {
      case true => Some(parseFiniteDuration("sleep", client.getString("sleep")))
      case false => None
    }
  }

  object Pool {
    val Lifo = pool.getBoolean("lifo")
    val MaxActive = pool.getInt("max-active")
    val MaxIdle = pool.getInt("max-idle")
    
    val WhenExhaustedAction = pool.getString("when-exhausted-action").toLowerCase match {
      case "fail"   => GenericObjectPool.WHEN_EXHAUSTED_FAIL
      case "grow"   => GenericObjectPool.WHEN_EXHAUSTED_GROW
      case "block"  => GenericObjectPool.WHEN_EXHAUSTED_BLOCK
      case _        => throw new IllegalArgumentException(
        "when-exhausted-action can only be fail, grow or block"
      )
    }
    val MaxWait = parseDuration(pool.getString("max-wait"))
    
    val TestWithPing = pool.getBoolean("test-with-ping")
    
    val TestOnBorrow = pool.getBoolean("test-on-borrow")
    val TestOnReturn = pool.getBoolean("test-on-return")
    
    object Eviction {
      val EvictionRunInterval = if(poolEviction.hasPath("eviction-run-interval")) {
        Some(parseFiniteDuration(
          "eviction-run-interval",
          poolEviction.getString("eviction-run-interval")
        ))
      } else {
        None
      }
      val EvictableAfter = if(poolEviction.hasPath("evictable-after")) {
        Some(parseFiniteDuration(
          "evictable-after",
          poolEviction.getString("evictable-after")
        ))
      } else {
        None
      }
      val SoftEvictableAfter = if(poolEviction.hasPath("soft-evictable-after")) {
        Some(parseFiniteDuration(
          "soft-evictable-after",
          poolEviction.getString("soft-evictable-after")
        ))
      } else {
        None
      }
      val MinIdle = poolEviction.getInt("min-idle")
      val TestsPerEvictionRun = poolEviction.getInt("tests-per-eviction-run")
      val TestWhileIdle = poolEviction.getBoolean("test-while-idle")
    }
    
    val Config = new GenericObjectPool.Config()
    Config.lifo = Lifo
    Config.maxActive = MaxActive
    Config.maxIdle = MaxIdle
    Config.minIdle = Eviction.MinIdle
    Config.whenExhaustedAction = WhenExhaustedAction
    Config.maxWait = if(MaxWait.isFinite) {
      FiniteDuration(MaxWait.length, MaxWait.unit).toMillis
    } else {
      -1
    }
    Config.testOnBorrow = TestOnBorrow
    Config.testOnReturn = TestOnReturn
    Config.timeBetweenEvictionRunsMillis = Eviction.EvictionRunInterval match {
      case Some(duration) => duration.toMillis
      case None => -1
    }
    Config.minEvictableIdleTimeMillis = Eviction.EvictableAfter match {
      case Some(duration) => duration.toMillis
      case None => -1
    }
    Config.softMinEvictableIdleTimeMillis = Eviction.SoftEvictableAfter match {
      case Some(duration) => duration.toMillis
      case None => -1
    }
    Config.numTestsPerEvictionRun = Eviction.TestsPerEvictionRun
    Config.testWhileIdle = Eviction.TestWhileIdle
  }

  object Async {
    val IsAutomaticPipeliningEnabled = async.getBoolean("auto-pipeline")
    val Threshold = async.hasPath("auto-pipeline-threshold") match {
      case true => async.getInt("auto-pipeline-threshold") match {
        case x if x == 0 => None
        case x => Some(x)
      }
      case false => None
    }
    val Interval = parseFiniteDuration(
      "auto-pipeline-interval",
      async.getString("auto-pipeline-interval")
    )
    val TimerThreadNamingPattern = async.getString("timer-thread-naming-pattern")
    
    object Executors {
      val Threads = asyncExecutors.getInt("threads")
      val QueueCapacity = asyncExecutors.getInt("queue-capacity")
      val ThreadsNamingPattern = asyncExecutors.getString("threads-naming-pattern")
      val ThreadsPriority = asyncExecutors.getString("threads-priority").toLowerCase match {
        case "min"    => Thread.MIN_PRIORITY
        case "normal" => Thread.NORM_PRIORITY
        case "max"    => Thread.MAX_PRIORITY
        case _        => throw new IllegalArgumentException(
          "threads-priority can only be min, normal or max"
        )
      }
    }
  }
  
  // Initialization
  Client
  Pool
  Async

}

object RedisConfig {
  def apply(config: Config = ConfigFactory.load().getConfig("scredis")) = new RedisConfig(config)
  def apply(config: Config, path: String) = new RedisConfig(config, path)
  def apply(configName: String) = new RedisConfig(configName)
  def apply(configName: String, path: String) = new RedisConfig(configName, path)
}

object RedisConfigDefaults {
  val Config = new RedisConfig()
  val Client = Config.Client
  val Pool = Config.Pool
  val Async = Config.Async
}