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

import akka.util.duration._

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import scredis.Redis
import scredis.exceptions.RedisCommandException
import scredis.tags._

import scala.collection.mutable.ListBuffer

class KeysCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val client = Redis()
  private val client2 = Redis("application.conf", "authenticated.scredis")
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private var dumpedValue: Array[Byte] = _

  private def setUpSortData(): Unit = {
    client.rPush("LIST", "1")
    client.rPush("LIST", "3")
    client.rPush("LIST", "5")
    client.rPush("LIST", "2")
    client.rPush("LIST", "4")
    client.rPush("LIST-ALPHA", "A")
    client.rPush("LIST-ALPHA", "C")
    client.rPush("LIST-ALPHA", "E")
    client.rPush("LIST-ALPHA", "B")
    client.rPush("LIST-ALPHA", "D")

    client.sAdd("SET", "1")
    client.sAdd("SET", "3")
    client.sAdd("SET", "5")
    client.sAdd("SET", "2")
    client.sAdd("SET", "4")
    client.sAdd("SET-ALPHA", "A")
    client.sAdd("SET-ALPHA", "C")
    client.sAdd("SET-ALPHA", "E")
    client.sAdd("SET-ALPHA", "B")
    client.sAdd("SET-ALPHA", "D")

    client.zAdd("ZSET", ("1", 5))
    client.zAdd("ZSET", ("3", 3))
    client.zAdd("ZSET", ("5", 1))
    client.zAdd("ZSET", ("2", 4))
    client.zAdd("ZSET", ("4", 2))
    client.zAdd("ZSET-ALPHA", ("A", 5))
    client.zAdd("ZSET-ALPHA", ("C", 3))
    client.zAdd("ZSET-ALPHA", ("E", 1))
    client.zAdd("ZSET-ALPHA", ("B", 4))
    client.zAdd("ZSET-ALPHA", ("D", 2))

    client.set("WEIGHT-1", "1")
    client.set("WEIGHT-2", "2")
    client.set("WEIGHT-3", "3")
    client.set("WEIGHT-4", "4")
    client.set("WEIGHT-5", "5")

    client.set("WEIGHT-A", "1")
    client.set("WEIGHT-B", "2")
    client.set("WEIGHT-C", "3")
    client.set("WEIGHT-D", "4")
    client.set("WEIGHT-E", "5")

    client.set("VALUE-1", "V1")
    client.set("VALUE-2", "V2")
    client.set("VALUE-3", "V3")
    client.set("VALUE-4", "V4")
    client.set("VALUE-5", "V5")

    client.set("VALUE-A", "VA")
    client.set("VALUE-B", "VB")
    client.set("VALUE-C", "VC")
    client.set("VALUE-D", "VD")
    client.set("VALUE-E", "VE")
    client.flushAutomaticPipeline()
  }

  import Names._
  import scredis.util.TestUtils._
  import client.ec

  Del when {
    "deleting a single key that does not exist" should {
      "return 0" taggedAs (V100) in {
        client.del("I-DONT-EXIST") must be(0)
      }
    }
    "deleting a single key that exists" should {
      "delete the key" taggedAs (V100) in {
        client.set("I-EXIST", "YES")
        client.del("I-EXIST") must be(1)
        client.del("I-EXIST") must be(0)
      }
    }
    "deleting multiple keys that partially exist" should {
      "delete the existing keys" taggedAs (V100) in {
        client.set("I-EXIST", "YES")
        client.set("I-EXIST-2", "YES")
        client.del("I-EXIST", "I-EXIST-2", "I-EXIST-3") must be(2)
        client.del("I-EXIST", "I-EXIST-2", "I-EXIST-3") must be(0)
      }
    }
  }

  Dump when {
    "the key does not exist" should {
      "return None" taggedAs (V260) in {
        client.dump("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key exists" should {
      "return the serialized value for that key" taggedAs (V260) in {
        client.set("TO-DUMP", SomeValue)
        val dump = client.dump("TO-DUMP").!
        dump must be('defined)
        dumpedValue = dump.get
      }
    }
  }

  Restore when {
    "the destination key already exists" should {
      "return an error" taggedAs (V260) in {
        evaluating {
          client.restore("TO-DUMP", dumpedValue)
        } must produce[RedisCommandException]
      }
    }
    "the destination key does not exist" should {
      "restore the dumped value" taggedAs (V260) in {
        client.restore("RESTORED", dumpedValue)
        client.get("RESTORED").map(_.get) must be(client.get("TO-DUMP").!.get)
        client.del("RESTORED")
      }
    }
    "applying a ttl" should {
      "restore the dumped value which should expire after the ttl" taggedAs (V260) in {
        client.restore("RESTORED", dumpedValue, Some(500 milliseconds))
        client.get("RESTORED").map(_.get) must be(client.get("TO-DUMP").!.get)
        Thread.sleep(600)
        client.get("RESTORED") must be('empty)
      }
    }
  }

  Exists when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.exists("NONEXISTENTKEY") must be(false)
      }
    }
    "the key exists" should {
      "return true" taggedAs (V100) in {
        client.exists("TO-DUMP") must be(true)
      }
    }
  }

  Expire when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.expire("NONEXISTENTKEY", 1) must be(false)
      }
    }
    "the key exists" should {
      "return true and the target key must expire after the ttl" taggedAs (V100) in {
        client.set("TO-EXPIRE", "HEY")
        client.expire("TO-EXPIRE", 1) must be(true)
        client.get("TO-EXPIRE") must be('defined)
        Thread.sleep(1050)
        client.get("TO-EXPIRE") must be('empty)
      }
    }
  }

  PExpire when {
    "the key does not exist" should {
      "return false" taggedAs (V260) in {
        client.pExpire("NONEXISTENTKEY", 500) must be(false)
        client.expireFromDuration("NONEXISTENTKEY", 500 milliseconds) must be(false)
      }
    }
    "the key exists" should {
      "return true and the target key must expire after the ttl" taggedAs (V260) in {
        client.set("TO-EXPIRE", "HEY")
        client.pExpire("TO-EXPIRE", 500) must be(true)
        client.get("TO-EXPIRE") must be('defined)
        Thread.sleep(550)
        client.get("TO-EXPIRE") must be('empty)

        client.set("TO-EXPIRE", "HEY")
        client.expireFromDuration("TO-EXPIRE", 500 milliseconds) must be(true)
        client.get("TO-EXPIRE") must be('defined)
        Thread.sleep(550)
        client.get("TO-EXPIRE") must be('empty)
      }
    }
  }

  ExpireAt when {
    "the key does not exist" should {
      "return false" taggedAs (V120) in {
        val unixTimestamp = (System.currentTimeMillis / 1000)
        client.expireAt("NONEXISTENTKEY", unixTimestamp) must be(false)
      }
    }
    "the key exists" should {
      "return true and the target key must expire after the ttl" taggedAs (V120) in {
        val unixTimestamp = (System.currentTimeMillis / 1000) + 1
        client.set("TO-EXPIRE", "HEY")
        client.expireAt("TO-EXPIRE", unixTimestamp) must be(true)
        client.get("TO-EXPIRE") must be('defined)
        Thread.sleep(1050)
        client.get("TO-EXPIRE") must be('empty)
      }
    }
  }

  PExpireAt when {
    "the key does not exist" should {
      "return false" taggedAs (V260) in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        client.pExpireAt("NONEXISTENTKEY", unixTimestampMillis) must be(false)
      }
    }
    "the key exists" should {
      "return true and the target key must expire after the ttl" taggedAs (V260) in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        client.set("TO-EXPIRE", "HEY")
        client.pExpireAt("TO-EXPIRE", unixTimestampMillis) must be(true)
        client.get("TO-EXPIRE") must be('defined)
        Thread.sleep(550)
        client.get("TO-EXPIRE") must be('empty)
      }
    }
  }

  Keys when {
    "the database is empty" should {
      "return None" taggedAs (V100) in {
        client.flushAll()
        client.keys("*") must be('empty)
      }
    }
    "the database contains three keys" should {
      "return the keys accordingly" taggedAs (V100) in {
        client.set("a", "a")
        client.set("ab", "b")
        client.set("abc", "c")
        client.keys("*").! must have size (3)
        client.keys("a*").! must have size (3)
        client.keys("ab*").! must have size (2)
        client.keys("abc*").! must have size (1)
        client.keys("ab").! must have size (1)
      }
    }
  }

  Migrate when {
    "the key does not exist" should {
      "return an error" taggedAs (V260) in {
        evaluating {
          client.migrate("NONEXISTENTKEY", "127.0.0.1")
        } must produce[RedisCommandException]
      }
    }
    "migrating a key to the same instance" should {
      "return an error" taggedAs (V260) in {
        client.set("TO-MIGRATE", SomeValue).!
        evaluating {
          client.migrate("TO-MIGRATE", "127.0.0.1", timeout = 500 milliseconds)
        } must produce[RedisCommandException]
      }
    }
    "migrating a key to a non-existing instance" should {
      "return an error" taggedAs (V260) in {
        evaluating {
          client.migrate("TO-MIGRATE", "127.0.0.1", 6378, timeout = 500 milliseconds)
        } must produce[RedisCommandException]
      }
    }
    "migrating a key to a valid instance" should {
      "succeed" taggedAs (V260) in {
        // Remove password on 6380
        client2.configSet("requirepass", "").!
        client2.auth("").!
        client.migrate("TO-MIGRATE", "127.0.0.1", 6380, timeout = 500 milliseconds).!
        client.exists("TO-MIGRATE") must be(false)
        client2.get("TO-MIGRATE").map(_.get) must be(SomeValue)
        // Set password back on 6380
        client2.configSet("requirepass", "foobar").!
        client2.auth("foobar").!
      }
    }
    "migrating a key to a valid instance in another database" should {
      "succeed" taggedAs (V260) in {
        client2.migrate("TO-MIGRATE", "127.0.0.1", 6379, database = 1, timeout = 500 milliseconds).!
        client2.exists("TO-MIGRATE") must be(false)
        client.select(1).!
        client.get("TO-MIGRATE").map(_.get) must be(SomeValue)
        client.flushDb().!
        client.select(0).!
      }
    }
    "migrating a key to a valid instance with COPY option enabled" should {
      "succeed and keep the original key" is (pending)
    }
    "migrating a key to a valid instance with REPLACE option enabled" should {
      "succeed and replace the target key" is (pending)
    }
  }

  Move when {
    "moving a key that does not exist" should {
      "return false" taggedAs (V100) in {
        client.move("NONEXISTENTKEY", 1) must be(false)
      }
    }
    "moving a key from database 0 to 1" should {
      Given("that the key does not exist in database 1 yet")
      "succeed" taggedAs (V100) in {
        client.set("TO-MOVE", SomeValue).!
        client.move("TO-MOVE", 1) must be(true)
        client.exists("TO-MOVE") must be(false)
        client.select(1).!
        client.get("TO-MOVE").map(_.get) must be(SomeValue)
      }
      Given("that the key already exists in database 1")
      "fail" taggedAs (V100) in {
        client.select(0).!
        client.set("TO-MOVE", SomeValue).!
        client.move("TO-MOVE", 1) must be(false)
        client.exists("TO-MOVE") must be(true)
        client.flushAll().!
      }
    }
  }

  Persist when {
    "persisting a non-existent key" should {
      "return false" taggedAs (V220) in {
        client.persist("NONEXISTENTKEY") must be(false)
      }
    }
    "persisting a key that has no ttl" should {
      "return false" taggedAs (V220) in {
        client.set("TO-PERSIST", SomeValue)
        client.persist("TO-PERSIST") must be(false)
      }
    }
    "persisting a key that has a ttl" should {
      "return true and the key should not expire" taggedAs (V220) in {
        client.expireFromDuration("TO-PERSIST", 500 milliseconds)
        client.persist("TO-PERSIST") must be(true)
        Thread.sleep(550)
        client.exists("TO-PERSIST") must be(true)
      }
    }
  }

  Ttl when {
    "key does not exist" should {
      "return None" taggedAs (V100) in {
        client.ttl("NONEXISTENTKEY") must be(Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return None" taggedAs (V100) in {
        client.set("TO-TTL", SomeValue)
        client.ttl("TO-TTL") must be(Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return None" taggedAs (V100) in {
        client.expire("TO-TTL", 1)
        client.ttl("TO-TTL").map(_.right.get) must be <= (1)
        client.del("TO-TTL")
      }
    }
  }

  PTtl when {
    "key does not exist" should {
      "return None" taggedAs (V260) in {
        client.pTtl("NONEXISTENTKEY") must be(Left(false))
        client.ttlDuration("NONEXISTENTKEY") must be(Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return None" taggedAs (V260) in {
        client.set("TO-TTL", SomeValue)
        client.pTtl("TO-TTL") must be(Left(true))
        client.ttlDuration("TO-TTL") must be(Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return None" taggedAs (V260) in {
        client.pExpire("TO-TTL", 500)
        client.pTtl("TO-TTL").map(_.right.get) must be <= (500L)
        (client.ttlDuration("TO-TTL").!.right.get <= (500 milliseconds)) must be(true)
        client.del("TO-TTL")
      }
    }
  }

  RandomKey when {
    "the database is empty" should {
      "return None" taggedAs (V100) in {
        client.flushDb()
        client.randomKey() must be('empty)
      }
    }
    "the database has some keys" should {
      "return None" taggedAs (V100) in {
        client.set("key1", "value1")
        client.set("key2", "value2")
        client.set("key3", "value3")
        client.randomKey().map(_.get) must (be("key1") or be("key2") or be("key3"))
      }
    }
  }

  Rename when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        evaluating {
          client.rename("sourceKey", "destKey")
        } must produce[RedisCommandException]
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", SomeValue)
        evaluating {
          client.rename("sourceKey", "sourceKey")
        } must produce[RedisCommandException]
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs (V100) in {
        client.rename("sourceKey", "destKey")
        client.exists("sourceKey") must be(false)
        client.get("destKey").map(_.get) must be(SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "succeed and overwrite destKey" taggedAs (V100) in {
        client.set("sourceKey", "OTHERVALUE")
        client.rename("sourceKey", "destKey")
        client.exists("sourceKey") must be(false)
        client.get("destKey").map(_.get) must be("OTHERVALUE")
      }
    }
  }

  RenameNX when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        client.del("sourceKey", "destKey")
        evaluating {
          client.renameNX("sourceKey", "destKey")
        } must produce[RedisCommandException]
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", SomeValue)
        evaluating {
          client.renameNX("sourceKey", "sourceKey")
        } must produce[RedisCommandException]
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs (V100) in {
        client.renameNX("sourceKey", "destKey") must be(true)
        client.exists("sourceKey") must be(false)
        client.get("destKey").map(_.get) must be(SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", "OTHERVALUE")
        client.renameNX("sourceKey", "destKey") must be(false)
      }
    }
  }

  Sort when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        setUpSortData()
        client.sort("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key contains a list" should {
      Given("that the list contains numbers")
      "sort the list accordingly" taggedAs (V100) in {
        client.sort("LIST", by = Some("NOSORT")) must be(List(
          Some("1"), Some("3"), Some("5"), Some("2"), Some("4")
        ))
        client.sort("LIST") must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("LIST", desc = true) must be(List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        ))
        client.sort[String]("LIST", desc = true, limit = Some((1, 2))) must be(List(
          Some("4"), Some("3")
        ))
        client.sort("LIST", by = Some("WEIGHT-*")) must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("LIST", get = List("VALUE-*")) must be(List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        ))
        client.sort[String]("LIST", get = List("VALUE-*", "WEIGHT-*")) must be(List(
          Some("V1"), Some("1"), Some("V2"), Some("2"), Some("V3"), Some("3"),
          Some("V4"), Some("4"), Some("V5"), Some("5")
        ))
      }
      Given("that the list contains strings")
      "lexicographically sort the list" taggedAs (V100) in {
        client.sort[String]("LIST-ALPHA", alpha = true, by = Some("NOSORT")) must be(List(
          Some("A"), Some("C"), Some("E"), Some("B"), Some("D")
        ))
        client.sort[String]("LIST-ALPHA", alpha = true) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("LIST-ALPHA", alpha = true, desc = true) must be(List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        ))
        client.sort[String]("LIST-ALPHA", alpha = true, desc = true, limit = Some((1, 2))) must be(
          List(Some("D"), Some("C"))
        )
        client.sort[String]("LIST-ALPHA", alpha = true, by = Some("WEIGHT-*")) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("LIST-ALPHA", alpha = true, get = List("VALUE-*")) must be(List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        ))
        client.sort[String]("LIST-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")) must be(
          List(
            Some("VA"),
            Some("1"),
            Some("VB"),
            Some("2"),
            Some("VC"),
            Some("3"),
            Some("VD"),
            Some("4"),
            Some("VE"),
            Some("5")
          )
        )
      }
    }
    "the key contains a set" should {
      Given("that the set contains numbers")
      "sort the set accordingly" taggedAs (V100) in {
        client.sort("SET", by = Some("NOSORT")).map(_.flatten) must (
          contain("1") and contain("3") and contain("5") and contain("2") and contain("4")
        )
        client.sort("SET") must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("SET", desc = true) must be(List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        ))
        client.sort[String]("SET", desc = true, limit = Some((1, 2))) must be(List(
          Some("4"), Some("3")
        ))
        client.sort[String]("SET", by = Some("WEIGHT-*")) must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("SET", get = List("VALUE-*")) must be(List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        ))
        client.sort[String]("SET", get = List("VALUE-*", "WEIGHT-*")) must be(List(
          Some("V1"),
          Some("1"),
          Some("V2"),
          Some("2"),
          Some("V3"),
          Some("3"),
          Some("V4"),
          Some("4"),
          Some("V5"),
          Some("5")
        ))
      }
      Given("that the set contains strings")
      "lexicographically sort the set" taggedAs (V100) in {
        client.sort[String]("SET-ALPHA", alpha = true, by = Some("NOSORT")).map(_.flatten) must (
          contain("A") and contain("C") and contain("E") and contain("B") and contain("D")
        )
        client.sort[String]("SET-ALPHA", alpha = true) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("SET-ALPHA", alpha = true, desc = true) must be(List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        ))
        client.sort[String]("SET-ALPHA", alpha = true, desc = true, limit = Some((1, 2))) must be(
          List(Some("D"), Some("C"))
        )
        client.sort[String]("SET-ALPHA", alpha = true, by = Some("WEIGHT-*")) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("SET-ALPHA", alpha = true, get = List("VALUE-*")) must be(List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        ))
        client.sort[String]("SET-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")) must be(
          List(
            Some("VA"),
            Some("1"),
            Some("VB"),
            Some("2"),
            Some("VC"),
            Some("3"),
            Some("VD"),
            Some("4"),
            Some("VE"),
            Some("5")
          )
        )
      }
    }
    "the key contains a sorted set" should {
      Given("that the sorted set contains numbers")
      "sort the sorted set accordingly" taggedAs (V100) in {
        client.sort("ZSET", by = Some("NOSORT")) must be(List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        ))
        client.sort("ZSET") must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("ZSET", desc = true) must be(List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        ))
        client.sort[String]("ZSET", desc = true, limit = Some((1, 2))) must be(List(
          Some("4"), Some("3")
        ))
        client.sort[String]("ZSET", by = Some("WEIGHT-*")) must be(List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        ))
        client.sort[String]("ZSET", get = List("VALUE-*")) must be(List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        ))
        client.sort[String]("ZSET", get = List("VALUE-*", "WEIGHT-*")) must be(List(
          Some("V1"),
          Some("1"),
          Some("V2"),
          Some("2"),
          Some("V3"),
          Some("3"),
          Some("V4"),
          Some("4"), 
          Some("V5"),
          Some("5")
        ))
      }
      Given("that the sorted set contains strings")
      "lexicographically sort the sorted set" taggedAs (V100) in {
        client.sort[String]("ZSET-ALPHA", alpha = true, by = Some("NOSORT")) must be(List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        ))
        client.sort[String]("ZSET-ALPHA", alpha = true) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("ZSET-ALPHA", alpha = true, desc = true) must be(List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        ))
        client.sort[String]("ZSET-ALPHA", alpha = true, desc = true, limit = Some((1, 2))) must be(
          List(Some("D"), Some("C"))
        )
        client.sort[String]("ZSET-ALPHA", alpha = true, by = Some("WEIGHT-*")) must be(List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        ))
        client.sort[String]("ZSET-ALPHA", alpha = true, get = List("VALUE-*")) must be(List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        ))
        client.sort[String]("ZSET-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")) must be(
          List(
            Some("VA"),
            Some("1"),
            Some("VB"),
            Some("2"), 
            Some("VC"),
            Some("3"),
            Some("VD"),
            Some("4"),
            Some("VE"),
            Some("5")
          )
        )
      }
    }
  }

  Type when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.`type`("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key is a string" should {
      "return string" taggedAs (V100) in {
        client.set("STRING", "HELLO")
        client.`type`("STRING") must be(Some("string"))
        client.del("STRING")
      }
    }
    "the key is a hash" should {
      "return string" taggedAs (V100) in {
        client.hSet("HASH")("FIELD", "VALUE")
        client.`type`("HASH") must be(Some("hash"))
        client.del("HASH")
      }
    }
    "the key is a list" should {
      "return string" taggedAs (V100) in {
        client.rPush("LIST", "HELLO")
        client.`type`("LIST") must be(Some("list"))
        client.del("LIST")
      }
    }
    "the key is a set" should {
      "return string" taggedAs (V100) in {
        client.sAdd("SET", "HELLO")
        client.`type`("SET") must be(Some("set"))
        client.del("SET")
      }
    }
    "the key is a sorted set" should {
      "return string" taggedAs (V100) in {
        client.zAdd("SORTED-SET", ("HELLO", 0))
        client.`type`("SORTED-SET") must be(Some("zset"))
        client.del("SORTED-SET")
      }
    }
  }
  
  Scan when {
    "the database is empty" should {
      "return an empty set" taggedAs (V280) in {
        client.flushDb().!
        val (next, set) = client.scan(0).!
        next must be(0)
        set must be ('empty)
      }
    }
    "the database contains 5 keys" should {
      "return all keys" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.set("key" + i, SomeValue).!
        }
        val (next, set) = client.scan(0).!
        next must be(0)
        set must (
          contain("key1") and
          contain("key2") and
          contain("key3") and
          contain("key4") and
          contain("key5")
        )
        for (i <- 1 to 10) {
          client.set("foo" + i, SomeValue).!
        }
      }
    }
    "the database contains 15 keys" should {
      val full = ListBuffer[String]()
      for (i <- 1 to 5) {
        full += ("key" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullSortedList = full.toList.sorted
      
      Given("that no pattern is set")
      "return all keys" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(cursor).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList.sorted must be(fullSortedList)
      }
      Given("that a pattern is set")
      "return all matching keys" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan[String](cursor, matchOpt = Some("foo*")).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList.sorted must be(fullSortedList.filter(_.startsWith("foo")))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching keys in one iteration" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan[String](
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size must be(10)
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList.sorted must be(fullSortedList.filter(_.startsWith("foo")))
      }
    }
  }

  override def afterAll() {
    client.sync(_.flushAll())
    client2.sync(_.flushAll())
    client.quit()
    client2.quit()
  }

}