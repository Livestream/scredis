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
package scredis.commands

import org.scalatest.{ConfigMap, WordSpec, GivenWhenThen, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers._

import scredis.{ Redis, Client }
import scredis.exceptions._
import scredis.tags._

import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._
import scala.util.{ Success, Failure }

import java.util.concurrent.TimeoutException

class TransactionalCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val client = Client()
  private val client2 = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"
  private val RedisException = RedisCommandException(
    "WRONGTYPE Operation against a key holding the wrong kind of value"
  )
  private val Result = List(Success(), Success(Some(SomeValue)), Failure(RedisException))
  private val PingResult = List(Success("PONG"))

  import redis.ec
  
  override def beforeAll() {
    client.lPush("LIST", "A")
  }

  import Names._
  import scredis.util.TestUtils.futureToRichFuture

  "Pipelining" when {
    "using pipeline()" should {
      "work as epected" taggedAs (V100) in {
        val p = client.pipeline()
        val set = p.set("STR", SomeValue)
        val get = p.get("STR")
        val error = p.get("LIST")
        evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
        evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
        evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
        p.sync().toList must be(Result)
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        evaluating { p.sync() } must produce[RedisProtocolException]

        val p1 = client.pipeline()
        val p2 = client.pipeline()
        p1.sync() must be('empty)
        p2.sync() must be('empty)
        client.del("STR")
      }
    }
    "using pipelined()" should {
      "work as epected" taggedAs (V100) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.pipelined { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result.toList must be(Result)
        client.del("STR")
        
        redis.pipelined(_.ping()).! must be(PingResult)
      }
    }
    "using pipelined1()" should {
      "work as epected" taggedAs (V100) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.pipelined1 { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          get
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result must be(Some(SomeValue))
        client.del("STR")

        val result2 = client.pipelined1 { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          p.sync().toList must be(Result)
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          get
        }
        result2 must be(Some(SomeValue))
        client.del("STR")
        
        redis.pipelined1(_.ping()).! must be("PONG")
      }
    }
    "using pipelinedN()" should {
      "work as epected" taggedAs (V100) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.pipelinedN { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          List(get, p.exists("STR"))
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result.toList must be(List(Some(SomeValue), true))
        client.del("STR")

        val result2 = client.pipelinedN { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          val exists = p.exists("STR")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          p.sync().toList must be(Result ::: List(Success(true)))
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          List(get, exists)
        }
        result2.toList must be(List(Some(SomeValue), true))
        client.del("STR")
        
        redis.pipelinedN(p => List(p.ping(), p.ping())).! must be(List("PONG", "PONG"))
      }
    }
    "using pipelinedM()" should {
      "work as epected" taggedAs (V100) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.pipelinedM { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          Map("get" -> get, "exists" -> p.exists("STR"))
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result must be(Map("get" -> Some(SomeValue), "exists" -> true))
        client.del("STR")

        val result2 = client.pipelinedM { p =>
          set = p.set("STR", SomeValue)
          get = p.get("STR")
          error = p.get("LIST")
          val exists = p.exists("STR")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          p.sync().toList must be(Result ::: List(Success(true)))
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          Map("get" -> get, "exists" -> exists)
        }
        result2 must be(Map("get" -> Some(SomeValue), "exists" -> true))
        client.del("STR")
        
        redis.pipelinedM(p => Map("p1" -> p.ping(), "p2" -> p.ping())).! must be(
          Map("p1" -> "PONG", "p2" -> "PONG")
        )
      }
    }
  }

  "Transactions" when {
    "using multi()" should {
      Given("that nothing is watched and discard() is not called")
      "work as epected" taggedAs (V200) in {
        val m = client.multi()
        val set = m.set("STR", SomeValue)
        val get = m.get("STR")
        val error = m.get("LIST")
        evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
        evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
        evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
        m.exec().toList must be(Result)
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        evaluating { m.exec() } must produce[RedisProtocolException]

        val m1 = client.multi()
        evaluating { client.multi() } must produce[RedisCommandException]
        m1.exec() must be('empty)
        client.del("STR")
      }
      Given("that discard() is called before the end of the transaction")
      "abort the transaction" taggedAs (V200) in {
        val m = client.multi()
        val set = m.set("STR", SomeValue)
        m.discard()
        evaluating { m.get("STR") } must produce[RedisProtocolException]
        evaluating { m.exec() } must produce[RedisProtocolException]
        client.get("STR") must be('empty)
      }
      Given("that a watched key has been modified")
      "also abort the transaction" taggedAs (V200) in {
        client.watch("STR")
        val m = client.multi()
        m.set("STR", SomeValue)
        m.get("STR")
        client2.set("STR", "NO!")
        evaluating { m.exec() } must produce[RedisTransactionException] 
        client.get("STR") must be(Some("NO!"))
        client.del("STR")
      }
      Given("that a watched but then unwatched key has been modified")
      "succeed" taggedAs (V200) in {
        client.watch("STR")
        client.unWatch()
        val m = client.multi()
        m.set("STR", SomeValue)
        m.get("STR")
        m.get("LIST")
        client2.set("STR", "NO!")
        m.exec().toList must be(Result)
        client.get("STR") must be(Some(SomeValue))
        client.del("STR")
      }
    }
    "using transactional()" should {
      "work as epected" taggedAs (V200) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.transactional { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result.toList must be(Result)
        client.del("STR")
        
        redis.transactional(_.ping()).! must be(PingResult)
      }
    }
    "using transactional1()" should {
      "work as epected" taggedAs (V200) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.transactional1 { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          get
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result must be(Some(SomeValue))
        client.del("STR")

        val result2 = client.transactional1 { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          m.exec().toList must be(Result)
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          get
        }
        result2 must be(Some(SomeValue))
        client.del("STR")
        
        redis.transactional1(_.ping()).! must be("PONG")
      }
    }
    "using transactionalN()" should {
      "work as epected" taggedAs (V200) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.transactionalN { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          List(get, m.exists("STR"))
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result.toList must be(List(Some(SomeValue), true))
        client.del("STR")

        val result2 = client.transactionalN { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          val exists = m.exists("STR")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          m.exec().toList must be(Result ::: List(Success(true)))
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          List(get, exists)
        }
        result2.toList must be(List(Some(SomeValue), true))
        client.del("STR")
        
        redis.transactionalN(p => List(p.ping(), p.ping())).! must be(List("PONG", "PONG"))
      }
    }
    "using transactionalM()" should {
      "work as epected" taggedAs (V100) in {
        var set: Future[Unit] = null
        var get: Future[Option[String]] = null
        var error: Future[Option[String]] = null
        val result = client.transactionalM { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          Map("get" -> get, "exists" -> m.exists("STR"))
        }
        Await.result(set, 100 milliseconds) must be()
        Await.result(get, 100 milliseconds) must be(Some(SomeValue))
        evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
        result must be(Map("get" -> Some(SomeValue), "exists" -> true))
        client.del("STR")

        val result2 = client.transactionalM { m =>
          set = m.set("STR", SomeValue)
          get = m.get("STR")
          error = m.get("LIST")
          val exists = m.exists("STR")
          evaluating { Await.result(set, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(get, 100 milliseconds) } must produce[TimeoutException]
          evaluating { Await.result(error, 100 milliseconds) } must produce[TimeoutException]
          m.exec().toList must be(Result ::: List(Success(true)))
          Await.result(set, 100 milliseconds) must be()
          Await.result(get, 100 milliseconds) must be(Some(SomeValue))
          evaluating { Await.result(error, 100 milliseconds) } must produce[RedisException]
          Map("get" -> get, "exists" -> exists)
        }
        result2 must be(Map("get" -> Some(SomeValue), "exists" -> true))
        client.del("STR")
        
        redis.transactionalM(m => Map("p1" -> m.ping(), "p2" -> m.ping())).! must be(
          Map("p1" -> "PONG", "p2" -> "PONG")
        )
      }
    }
  }

  override def afterAll(configMap: ConfigMap) {
    redis.quit()
    client.flushAll()
    client.quit()
    client2.quit()
  }

}
