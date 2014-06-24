package scredis

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import scredis.exceptions.RedisCommandException
import scredis.tags._

import scala.concurrent. { Future, Await }
import scala.concurrent.duration._

class RedisSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  
  import redis.ec
  
  "Redis" when {
    "issuing some commands with auto-pipelining enabled" should {
      "end up executing them in a pipeline" taggedAs(V100) in {
        val c1 = redis.get("STR")(CommandOptions(timeout = 1 second, tries = 1, sleep = None))
        val c2 = redis.set("STR", "Hello World!")(CommandOptions(
          timeout = 3 seconds,
          tries = 3,
          sleep = Some(500 milliseconds)
        ))
        val c3 = redis.get("STR")(CommandOptions(
          timeout = 2 seconds,
          tries = 2,
          sleep = Some(1 second)
        ))
        val c4 = redis.get("STR")(CommandOptions(
          timeout = 2 seconds,
          tries = 2,
          sleep = Some(1 second),
          force = true
        ))
        Await.result(c4, Duration.Inf) must be('empty)
        Await.result(c1, Duration.Inf) must be('empty)
        Await.result(c2, Duration.Inf) must be(())
        Await.result(c3, Duration.Inf) must be(Some("Hello World!"))
        redis.sync(_.flushAll())
      }
    }
    "selecting a different database" should {
      "change the dabatase on all clients" taggedAs(V100) in {
        val keys = redis.keys("*")
        Await.result(keys, Duration.Inf) must be('empty)
        redis.selectSync(1)
        val set = redis.set("STR", "Hello")
        Await.result(set, Duration.Inf)
        val f1 = for(i <- (1 to 1000)) yield {
          redis.keys("*").map(x => x must be(Set("STR")))
        }
        Await.result(Future.sequence(f1), Duration.Inf)
        redis.selectSync(0)
        val f2 = for(i <- (1 to 1000)) yield {
          redis.keys("*").map(x => x must be('empty))
        }
        Await.result(Future.sequence(f2), Duration.Inf)
      }
    }
  }
  
  override def afterAll() {
    redis.sync(_.flushAll())
    redis.quit()
  }
  
}