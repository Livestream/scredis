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

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import scredis.Redis
import scredis.exceptions.RedisCommandException
import scredis.tags._

import scala.collection.mutable.{ Set => MutableSet }

class HashesCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val client = Redis()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll() = {
    client.lPush("LIST", "A")
  }

  import Names._
  import scredis.util.TestUtils._
  import client.ec

  HDel when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.hDel("NONEXISTENTKEY")("foo") must be(0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hDel("LIST")("foo") } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the field" should {
      "return 0" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hDel("HASH")("FIELD2") must be(0)
      }
    }
    "the hash contains the field" should {
      "return 1" taggedAs (V200) in {
        client.hDel("HASH")("FIELD") must be(1)
        client.hExists("HASH")("FIELD") must be(false)
      }
    }
  }

  "%s >= 2.4".format(HDel) when {
    "the key does not exist" should {
      "return 0" taggedAs (V240) in {
        client.hDel("NONEXISTENTKEY")("foo", "bar") must be(0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V240) in {
        evaluating { client.hDel("LIST")("foo", "bar") } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the fields" should {
      "return 0" taggedAs (V240) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hSet("HASH")("FIELD2", SomeValue)
        client.hSet("HASH")("FIELD3", SomeValue)
        client.hDel("HASH")("FIELD4", "FIELD5") must be(0)
      }
    }
    "the hash contains the fields" should {
      "return the correct number of deleted fields" taggedAs (V240) in {
        client.hDel("HASH")("FIELD", "FIELD4", "FIELD5") must be(1)
        client.hDel("HASH")("FIELD2", "FIELD3") must be(2)
        client.hExists("HASH")("FIELD") must be(false)
        client.hExists("HASH")("FIELD2") must be(false)
        client.hExists("HASH")("FIELD3") must be(false)
      }
    }
  }

  HExists when {
    "the key does not exist" should {
      "return false" taggedAs (V200) in {
        client.hExists("NONEXISTENTKEY")("foo") must be(false)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hExists("LIST")("foo") } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the field" should {
      "return false" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hExists("HASH")("FIELD2") must be(false)
      }
    }
    "the hash contains the field" should {
      "return true" taggedAs (V200) in {
        client.hExists("HASH")("FIELD") must be(true)
        client.hDel("HASH")("FIELD")
      }
    }
  }

  HGet when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hGet("NONEXISTENTKEY")("foo") must be('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hGet("LIST")("foo") } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the field" should {
      "return None" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hGet("HASH")("FIELD2") must be('empty)
      }
    }
    "the hash contains the field" should {
      "return the stored value" taggedAs (V200) in {
        client.hGet("HASH")("FIELD").map(_.get) must be(SomeValue)
        client.hDel("HASH")("FIELD")
      }
    }
  }

  HGetAll when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hGetAll("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hGetAll("LIST") } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "return the stored value" taggedAs (V200) in {
        client.hSet("HASH")("FIELD1", SomeValue)
        client.hSet("HASH")("FIELD2", "YES")
        client.hGetAll("HASH").map(_.get) must be(Map("FIELD1" -> SomeValue, "FIELD2" -> "YES"))
        client.del("HASH")
      }
    }
  }

  HIncrBy when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs (V200) in {
        client.hIncrBy("HASH")("FIELD", 1) must be(1)
        client.hGet("HASH")("FIELD").map(_.get.toInt) must be(1)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hIncrBy("LIST")("foo", 1) } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs (V200) in {
        client.hIncrBy("HASH")("FIELD2", 3) must be(3)
        client.hGet("HASH")("FIELD2").map(_.get.toInt) must be(3)
      }
    }
    "the hash contains the field but the latter does not contain an integer" should {
      "return an error" taggedAs (V200) in {
        client.hSet("HASH")("FIELD3", SomeValue)
        evaluating { client.hIncrBy("HASH")("FIELD3", 2) } must produce[RedisCommandException]
      }
    }
    "the hash contains the field and the latter is an integer" should {
      "increment the value" taggedAs (V200) in {
        client.hIncrBy("HASH")("FIELD", -3) must be(-2)
        client.hGet("HASH")("FIELD").map(_.get.toInt) must be(-2)
        client.del("HASH")
      }
    }
  }

  HIncrByFloat when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs (V260) in {
        client.hIncrByFloat("HASH")("FIELD", 1.2) must be(1.2)
        client.hGet("HASH")("FIELD").map(_.get.toDouble) must be(1.2)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V260) in {
        evaluating { client.hIncrByFloat("LIST")("foo", 1.2) } must produce[RedisCommandException]
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs (V260) in {
        client.hIncrByFloat("HASH")("FIELD2", 3.4) must be(3.4)
        client.hGet("HASH")("FIELD2").map(_.get.toDouble) must be(3.4)
      }
    }
    "the hash contains the field but the latter does not contain a floating point number" should {
      "return an error" taggedAs (V260) in {
        client.hSet("HASH")("FIELD3", SomeValue)
        evaluating {
          client.hIncrByFloat("HASH")("FIELD3", 2.1)
        } must produce[RedisCommandException]
      }
    }
    "the hash contains the field and the latter is a floating point number" should {
      "increment the value" taggedAs (V260) in {
        client.hIncrByFloat("HASH")("FIELD", -3.1) must be(-1.9)
        client.hGet("HASH")("FIELD").map(_.get.toDouble) must be(-1.9)
        client.del("HASH")
      }
    }
  }

  HKeys when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hKeys("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hKeys("LIST") } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs (V200) in {
        client.hSet("HASH")("FIELD1", SomeValue)
        client.hSet("HASH")("FIELD2", "YES")
        client.hKeys("HASH").map(_.toList) must be(List("FIELD1", "FIELD2"))
        client.del("HASH")
      }
    }
  }

  HLen when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.hLen("NONEXISTENTKEY") must be(0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hLen("LIST") } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "return the number of fields in the hash" taggedAs (V200) in {
        client.hSet("HASH")("FIELD1", SomeValue)
        client.hSet("HASH")("FIELD2", "YES")
        client.hLen("HASH") must be(2)
        client.del("HASH")
      }
    }
  }

  HMGet when {
    "the key does not exist" should {
      "return a list contianing only None" taggedAs (V200) in {
        client.hmGet("NONEXISTENTKEY")("foo", "bar") must be(List(None, None))
        client.hmGetAsMap("NONEXISTENTKEY")("foo", "bar") must be('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hmGet("LIST")("foo", "bar") } must produce[RedisCommandException]
        evaluating { client.hmGetAsMap("LIST")("foo", "bar") } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "return the values" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hSet("HASH")("FIELD2", "YES")
        client.hmGet("HASH")("FIELD", "FIELD2", "FIELD3") must be(
          List(Some(SomeValue), Some("YES"), None))
        client.hmGetAsMap("HASH")("FIELD", "FIELD2", "FIELD3") must be(
          Map("FIELD" -> SomeValue, "FIELD2" -> "YES"))
        client.del("HASH")
      }
    }
  }

  HMSet when {
    "the key does not exist" should {
      "create a hash and set all specified fields" taggedAs (V200) in {
        client.hmSet("HASH")(("FIELD", SomeValue), ("FIELD2", "YES"))
        client.hmGet("HASH")("FIELD", "FIELD2") must be(List(Some(SomeValue), Some("YES")))
        client.del("HASH")
        client.hmSetFromMap("HASH", Map("FIELD" -> SomeValue, "FIELD2" -> "YES"))
        client.hmGet("HASH")("FIELD", "FIELD2") must be(List(Some(SomeValue), Some("YES")))
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating {
          client.hmSet("LIST")(("FIELD", SomeValue), ("FIELD2", "YES"))
        } must produce[RedisCommandException]
        evaluating {
          client.hmSetFromMap("LIST", Map("FIELD" -> SomeValue, "FIELD2" -> "YES"))
        } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "set and overwrite fields" taggedAs (V200) in {
        client.hmSet("HASH")(("FIELD", "YES"), ("FIELD2", SomeValue), ("FIELD3", "NO"))
        client.hmGet("HASH")("FIELD", "FIELD2", "FIELD3") must be(
          List(Some("YES"), Some(SomeValue), Some("NO")))

        client.hmSetFromMap("HASH", Map("FIELD" -> "NO", "FIELD2" -> "", "FIELD3" -> "YES"))
        client.hmGet("HASH")("FIELD", "FIELD2", "FIELD3") must be(
          List(Some("NO"), Some(""), Some("YES")))
        client.del("HASH")
      }
    }
  }

  HSet when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", SomeValue)
        client.hGet("HASH")("FIELD") must be(Some(SomeValue))
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hSet("LIST")("foo", "bar") } must produce[RedisCommandException]
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs (V200) in {
        client.hSet("HASH")("FIELD2", "YES")
        client.hGet("HASH")("FIELD2") must be(Some("YES"))
      }
    }
    "the hash already contains the field" should {
      "overwrite the value" taggedAs (V200) in {
        client.hSet("HASH")("FIELD", "NEW")
        client.hGet("HASH")("FIELD") must be(Some("NEW"))
        client.del("HASH")
      }
    }
  }

  HSetNX when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs (V200) in {
        client.hSetNX("HASH")("FIELD", SomeValue) must be(true)
        client.hGet("HASH")("FIELD") must be(Some(SomeValue))
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hSetNX("LIST")("foo", "bar") } must produce[RedisCommandException]
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs (V200) in {
        client.hSetNX("HASH")("FIELD2", "YES") must be(true)
        client.hGet("HASH")("FIELD2") must be(Some("YES"))
      }
    }
    "the hash already contains the field" should {
      "return an error" taggedAs (V200) in {
        client.hSetNX("HASH")("FIELD", "NEW") must be(false)
        client.hGet("HASH")("FIELD") must be(Some(SomeValue))
        client.del("HASH")
      }
    }
  }

  HVals when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hVals("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        evaluating { client.hVals("LIST") } must produce[RedisCommandException]
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs (V200) in {
        client.hSet("HASH")("FIELD1", SomeValue)
        client.hSet("HASH")("FIELD2", "YES")
        client.hSet("HASH")("FIELD3", "YES")
        client.hVals("HASH") must be(List(SomeValue, "YES", "YES"))
        client.del("HASH")
      }
    }
  }
  
  HScan when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = client.hScan[String]("NONEXISTENTKEY")(0).!
        next must be(0)
        set must be ('empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V280) in {
        evaluating { client.hScan[String]("LIST")(0) } must produce[RedisCommandException]
      }
    }
    "the hash contains 5 fields" should {
      "return all fields" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.hSet("HASH")("key" + i, "value" + i).!
        }
        val (next, set) = client.hScan[String]("HASH")(0).!
        next must be(0)
        set must (
          contain(("key1", "value1")) and
          contain(("key2", "value2")) and
          contain(("key3", "value3")) and
          contain(("key4", "value4")) and
          contain(("key5", "value5"))
        )
        for (i <- 1 to 10) {
          client.hSet("HASH")("foo" + i, "foo" + i).!
        }
      }
    }
    "the hash contains 15 fields" should {
      val full = MutableSet[(String, String)]()
      for (i <- 1 to 5) {
        full += (("key" + i, "value" + i))
      }
      for (i <- 1 to 10) {
        full += (("foo" + i, "foo" + i))
      }
      val fullSet = full.toSet
      
      Given("that no pattern is set")
      "return all fields" taggedAs (V280) in {
        val elements = MutableSet[(String, String)]()
        var cursor = 0L
        do {
          val (next, set) = client.hScan[String]("HASH")(cursor).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet)
      }
      Given("that a pattern is set")
      "return all matching fields" taggedAs (V280) in {
        val elements = MutableSet[(String, String)]()
        var cursor = 0L
        do {
          val (next, set) = client.hScan[String]("HASH")(cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet.filter {
          case (key, value) => key.startsWith("foo")
        })
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching fields in one iteration" taggedAs (V280) in {
        val elements = MutableSet[(String, String)]()
        var cursor = 0L
        do {
          val (next, set) = client.hScan[String]("HASH")(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size must be(10)
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet.filter {
          case (key, value) => key.startsWith("foo")
        })
      }
    }
  }

  override def afterAll() {
    client.sync(_.flushDb())
    client.quit()
  }

}