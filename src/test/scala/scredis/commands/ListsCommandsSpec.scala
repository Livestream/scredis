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
import org.scalatest.MustMatchers._

import scredis.Redis
import scredis.exceptions.RedisCommandException
import scredis.tags._

import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._

import java.util.concurrent.Executors

class ListsCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  import scredis.util.TestUtils._
  import redis.ec

  override def beforeAll(): Unit = {
    redis.hSet("HASH")("FIELD", SomeValue).!
  }

  import Names._

  BLPop when {
    "the first existing key is not of type list" should {
      "return an error" taggedAs (V200) in {
        redis.rPush("LIST", "A").!
        evaluatingSync {
          redis.sync(_.blPop(1, "HASH", "LIST"))
        } must produce[RedisCommandException]
        redis.del("LIST").!
        evaluatingSync {
          redis.sync(_.blPop(1, "LIST", "HASH"))
        } must produce[RedisCommandException]
      }
    }
    "the keys do not exist or are empty" should {
      "succeed" taggedAs (V200) in {
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")) must be('empty)
      }
    }
    "the timeout is zero" should {
      "block and return as soon as a value is pushed" taggedAs (V200) in {
        val future = Future {
          val start = System.currentTimeMillis
          val result = redis.sync(_.blPop(0, "LIST", "LIST2", "LIST3"))
          val elapsed = System.currentTimeMillis - start
          (result, elapsed)
        }
        Thread.sleep(500)
        redis.rPush("LIST2", "A").!
        val (result, elapsed) = Await.result(future, 50 milliseconds)
        result.get must be(("LIST2" -> "A"))
        elapsed.toInt must be >= (500)
      }
    }
    "the keys contain a non-empty list" should {
      "pop the values in order" taggedAs (V200) in {
        redis.rPush("LIST", "A").!
        redis.rPush("LIST", "B").!
        redis.rPush("LIST2", "C").!
        redis.rPush("LIST3", "D").!
        redis.rPush("LIST3", "E").!
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get must be(("LIST" -> "A"))
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get must be(("LIST" -> "B"))
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get must be(("LIST2" -> "C"))
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get must be(("LIST3" -> "D"))
        redis.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get must be(("LIST3" -> "E"))
      }
    }
    "the BLPOP timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BLPOP timeout" taggedAs (V200) in {
        redis.sync { client =>
          client.setTimeout(200 milliseconds)
          client.blPop(1, "LIST", "LIST2", "LIST3") must be('empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }

  BRPop when {
    "the first existing key is not of type list" should {
      "return an error" taggedAs (V200) in {
        redis.lPush("LIST", "A").!
        evaluatingSync {
          redis.sync(_.brPop(1, "HASH", "LIST"))
        } must produce[RedisCommandException]
        redis.del("LIST").!
        evaluatingSync {
          redis.sync(_.brPop(1, "LIST", "HASH"))
        } must produce[RedisCommandException]
      }
    }
    "the keys do not exist or are empty" should {
      "succeed" taggedAs (V200) in {
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3")) must be('empty)
      }
    }
    "the timeout is zero" should {
      "block and return as soon as a value is pushed" taggedAs (V200) in {
        val future = Future {
          val start = System.currentTimeMillis
          val result = redis.sync(_.brPop(0, "LIST", "LIST2", "LIST3"))
          val elapsed = System.currentTimeMillis - start
          (result, elapsed)
        }
        Thread.sleep(500)
        redis.lPush("LIST2", "A").!
        val (result, elapsed) = Await.result(future, 50 milliseconds)
        result.get must be(("LIST2" -> "A"))
        elapsed.toInt must be >= (500)
      }
    }
    "the keys contain a non-empty list" should {
      "return the values in order" taggedAs (V200) in {
        redis.lPush("LIST", "A").!
        redis.lPush("LIST", "B").!
        redis.lPush("LIST2", "C").!
        redis.lPush("LIST3", "D").!
        redis.lPush("LIST3", "E").!
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get must be(("LIST" -> "A")))
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get must be(("LIST" -> "B")))
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get must be(("LIST2" -> "C")))
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get must be(("LIST3" -> "D")))
        redis.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get must be(("LIST3" -> "E")))
      }
    }
    "the BRPOP timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BRPOP timeout" taggedAs (V200) in {
        redis.sync { client =>
          client.setTimeout(200 milliseconds)
          client.brPop(1, "LIST", "LIST2", "LIST3") must be('empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }

  BRPopLPush when {
    "the source key does not exist" should {
      "do nothing" taggedAs (V220) in {
        redis.sync(_.brPopLPush("LIST", "LIST", 1)) must be('empty)
      }
    }
    "the source key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        evaluatingSync {
          redis.sync(_.brPopLPush("HASH", "LIST", 1))
        } must produce[RedisCommandException]
      }
    }
    "the dest key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        redis.lPush("LIST", "A").!
        evaluatingSync {
          redis.sync(_.brPopLPush("LIST", "HASH", 1))
        } must produce[RedisCommandException]
      }
    }
    "the source and dest keys are correct" should {
      "succeed" taggedAs (V220) in {
        redis.sync(_.brPopLPush("LIST", "LIST", 1)) must be(Some("A"))
        redis.sync(_.brPopLPush("LIST", "LIST2", 1)) must be(Some("A"))
        redis.lPop("LIST") must be('empty)
        redis.lPop("LIST2") must be(Some("A"))
      }
    }
    "the BRPopLPush timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BRPopLPush timeout" taggedAs (V200) in {
        redis.sync { client =>
          client.setTimeout(200 milliseconds)
          client.brPopLPush("LIST", "LIST", 1) must be('empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }

  LIndex when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.lIndex("LIST", 0) must be('empty)
        redis.lIndex("LIST", 100) must be('empty)
        redis.lIndex("LIST", -1) must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lIndex("HASH", 0) } must produce[RedisCommandException]
      }
    }
    "the index is out of range" should {
      "return None" taggedAs (V100) in {
        redis.rPush("LIST", "Hello")
        redis.rPush("LIST", "World")
        redis.rPush("LIST", "!")
        redis.lIndex("LIST", -100) must be('empty)
        redis.lIndex("LIST", 100) must be('empty)
      }
    }
    "the index is correct" should {
      "return the value stored at index" taggedAs (V100) in {
        redis.lIndex("LIST", 0) must be(Some("Hello"))
        redis.lIndex("LIST", 1) must be(Some("World"))
        redis.lIndex("LIST", 2) must be(Some("!"))

        redis.lIndex("LIST", -1) must be(Some("!"))
        redis.lIndex("LIST", -2) must be(Some("World"))
        redis.lIndex("LIST", -3) must be(Some("Hello"))
        redis.del("LIST")
      }
    }
  }

  LInsert when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        redis.lInsert("LIST", "A", SomeValue) must be(Some(0))
        redis.lPop("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        evaluating { redis.lInsert("HASH", "A", SomeValue) } must produce[RedisCommandException]
      }
    }
    "the pivot is not in the list" should {
      "return None" taggedAs (V220) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "C")
        redis.rPush("LIST", "E")
        redis.lInsert("LIST", "X", SomeValue) must be('empty)
      }
    }
    "the pivot is in the list" should {
      "insert the element at the correct position" taggedAs (V220) in {
        redis.lInsert("LIST", "A", "B", true) must be(Some(4))
        redis.lInsert("LIST", "E", "D", false) must be(Some(5))
        redis.lInsertAfter("LIST", "E", "G") must be(Some(6))
        redis.lInsertBefore("LIST", "G", "F") must be(Some(7))
        redis.lRange("LIST") must be(List("A", "B", "C", "D", "E", "F", "G"))
        redis.del("LIST")
      }
    }
  }

  LLen when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.lLen("LIST") must be(0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lLen("HASH") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "return the number of elements contained in the list" taggedAs (V100) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.lLen("LIST") must be(2)
        redis.del("LIST")
      }
    }
  }

  LPop when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.lPop("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lPop("HASH") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs (V100) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.lPop("LIST") must be(Some("A"))
        redis.lPop("LIST") must be(Some("B"))
        redis.lPop("LIST") must be('empty)
      }
    }
  }

  RPop when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.rPop("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.rPop("HASH") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs (V100) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.rPop("LIST") must be(Some("B"))
        redis.rPop("LIST") must be(Some("A"))
        redis.rPop("LIST") must be('empty)
      }
    }
  }

  LPush when {
    "the key does not exist" should {
      "create a list and prepend the value" taggedAs (V100) in {
        redis.lPush("LIST", "A") must be(1)
        redis.lPush("LIST", "B") must be(2)
        redis.lRange("LIST") must be(List("B", "A"))
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lPush("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs (V100) in {
        redis.lPush("LIST", "1") must be(3)
        redis.lPush("LIST", "2") must be(4)
        redis.lRange("LIST") must be(List("2", "1", "B", "A"))
        redis.del("LIST")
      }
    }
  }

  "%s-2.4".format(LPush) when {
    "the key does not exist" should {
      "create a list and prepend the values" taggedAs (V240) in {
        redis.lPush("LIST", "A", "B") must be(2)
        redis.lRange("LIST") must be(List("B", "A"))
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V240) in {
        evaluating { redis.lPush("HASH", "A", "B") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "prepend the values to the existing list" taggedAs (V240) in {
        redis.lPush("LIST", "1", "2") must be(4)
        redis.lRange("LIST") must be(List("2", "1", "B", "A"))
        redis.del("LIST")
      }
    }
  }

  RPush when {
    "the key does not exist" should {
      "create a list and append the value" taggedAs (V100) in {
        redis.rPush("LIST", "A") must be(1)
        redis.rPush("LIST", "B") must be(2)
        redis.lRange("LIST") must be(List("A", "B"))
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.rPush("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs (V100) in {
        redis.rPush("LIST", "1") must be(3)
        redis.rPush("LIST", "2") must be(4)
        redis.lRange("LIST") must be(List("A", "B", "1", "2"))
        redis.del("LIST")
      }
    }
  }

  "%s-2.4".format(RPush) when {
    "the key does not exist" should {
      "create a list and append the values" taggedAs (V240) in {
        redis.rPush("LIST", "A", "B") must be(2)
        redis.lRange("LIST") must be(List("A", "B"))
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V240) in {
        evaluating { redis.rPush("HASH", "A", "B") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "append the values to the existing list" taggedAs (V240) in {
        redis.rPush("LIST", "1", "2") must be(4)
        redis.lRange("LIST") must be(List("A", "B", "1", "2"))
        redis.del("LIST")
      }
    }
  }

  LPushX when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        redis.lPushX("LIST", "A") must be(0)
        redis.lPushX("LIST", "B") must be(0)
        redis.lRange("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        evaluating { redis.lPushX("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs (V220) in {
        redis.lPush("LIST", "3")
        redis.lPushX("LIST", "2") must be(2)
        redis.lPushX("LIST", "1") must be(3)
        redis.lRange("LIST") must be(List("1", "2", "3"))
        redis.del("LIST")
      }
    }
  }

  RPushX when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        redis.rPushX("LIST", "A") must be(0)
        redis.rPushX("LIST", "B") must be(0)
        redis.lRange("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        evaluating { redis.rPushX("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs (V220) in {
        redis.rPush("LIST", "1")
        redis.rPushX("LIST", "2") must be(2)
        redis.rPushX("LIST", "3") must be(3)
        redis.lRange("LIST") must be(List("1", "2", "3"))
        redis.del("LIST")
      }
    }
  }

  LRange when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.lRange("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lRange("HASH") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "return the elements in the specified range" taggedAs (V100) in {
        redis.rPush("LIST", "0")
        redis.rPush("LIST", "1")
        redis.rPush("LIST", "2")
        redis.rPush("LIST", "3")
        redis.lRange("LIST") must be(List("0", "1", "2", "3"))
        redis.lRange("LIST", 0, -1) must be(List("0", "1", "2", "3"))
        redis.lRange("LIST", 0, 0) must be(List("0"))
        redis.lRange("LIST", 1, 1) must be(List("1"))
        redis.lRange("LIST", 2, 2) must be(List("2"))
        redis.lRange("LIST", 3, 3) must be(List("3"))
        redis.lRange("LIST", 1, 2) must be(List("1", "2"))
        redis.lRange("LIST", -2, -1) must be(List("2", "3"))
        redis.lRange("LIST", -1, -1) must be(List("3"))
        redis.lRange("LIST", -4, -1) must be(List("0", "1", "2", "3"))
        redis.del("LIST")
      }
    }
  }

  LRem when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.lRem("LIST", "A") must be(0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lRem("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "delete the specified elements" taggedAs (V100) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.rPush("LIST", "C")
        redis.rPush("LIST", "D")
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.rPush("LIST", "C")
        redis.rPush("LIST", "D")

        redis.lRem("LIST", "X") must be(0)
        redis.lRange("LIST") must be(List("A", "B", "C", "D", "A", "B", "C", "D"))

        redis.lRem("LIST", "A") must be(2)
        redis.lRange("LIST") must be(List("B", "C", "D", "B", "C", "D"))

        redis.lRem("LIST", "B", 1) must be(1)
        redis.lRange("LIST") must be(List("C", "D", "B", "C", "D"))

        redis.lRem("LIST", "C", -1) must be(1)
        redis.lRange("LIST") must be(List("C", "D", "B", "D"))

        redis.lRem("LIST", "D", -2) must be(2)
        redis.lRange("LIST") must be(List("C", "B"))

        redis.lRem("LIST", "C", 50) must be(1)
        redis.lRange("LIST") must be(List("B"))

        redis.lRem("LIST", "B", -50) must be(1)
        redis.lRange("LIST") must be('empty)
      }
    }
  }

  LSet when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lSet("LIST", 0, "A") } must produce[RedisCommandException]
        evaluating { redis.lSet("LIST", 1, "A") } must produce[RedisCommandException]
        evaluating { redis.lSet("LIST", -1, "A") } must produce[RedisCommandException]
        redis.lRange("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lSet("HASH", 0, "A") } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      Given("the index is out of range")
      "return an error" taggedAs (V100) in {
        redis.rPush("LIST", "A")
        redis.rPush("LIST", "B")
        redis.rPush("LIST", "C")
        evaluating { redis.lSet("LIST", 3, "X") } must produce[RedisCommandException]
        evaluating { redis.lSet("LIST", -4, "X") } must produce[RedisCommandException]
        evaluating { redis.lSet("LIST", 55, "X") } must produce[RedisCommandException]
        redis.lRange("LIST") must be(List("A", "B", "C"))
      }
      Given("the index is correct")
      "set the provided values at the corresponding indices" taggedAs (V100) in {
        redis.lSet("LIST", 0, "D")
        redis.lSet("LIST", 1, "E")
        redis.lSet("LIST", 2, "F")
        redis.lRange("LIST") must be(List("D", "E", "F"))
        redis.lSet("LIST", -3, "A")
        redis.lSet("LIST", -2, "B")
        redis.lSet("LIST", -1, "C")
        redis.lRange("LIST") must be(List("A", "B", "C"))
        redis.del("LIST")
      }
    }
  }

  LTrim when {
    "the key does not exist" should {
      "do nothing" taggedAs (V100) in {
        redis.lTrim("LIST", 0, -1)
        redis.lRange("LIST") must be('empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        evaluating { redis.lTrim("HASH", 0, -1) } must produce[RedisCommandException]
      }
    }
    "the list contains some elements" should {
      "trim the list to the specified range" taggedAs (V100) in {
        redis.rPush("LIST", "0")
        redis.rPush("LIST", "1")
        redis.rPush("LIST", "2")
        redis.rPush("LIST", "3")

        redis.rPush("LIST2", "0")
        redis.rPush("LIST2", "1")
        redis.rPush("LIST2", "2")
        redis.rPush("LIST2", "3")

        redis.rPush("LIST3", "0")
        redis.rPush("LIST3", "1")
        redis.rPush("LIST3", "2")
        redis.rPush("LIST3", "3")

        redis.lTrim("LIST", 0, 0)
        redis.lRange("LIST") must be(List("0"))
        redis.lTrim("LIST2", 0, -1)
        redis.lRange("LIST2") must be(List("0", "1", "2", "3"))
        redis.lTrim("LIST2", -4, -1)
        redis.lRange("LIST2") must be(List("0", "1", "2", "3"))
        redis.lTrim("LIST2", 0, -3)
        redis.lRange("LIST2") must be(List("0", "1"))
        redis.lTrim("LIST3", 2, 3)
        redis.lRange("LIST3") must be(List("2", "3"))
        redis.lTrim("LIST3", 1, 1)
        redis.lRange("LIST3") must be(List("3"))
        redis.del("LIST", "LIST2", "LIST3")
      }
    }
  }

  RPopLPush when {
    "the source key does not exist" should {
      "return None and do nothing" taggedAs (V120) in {
        redis.rPopLPush("LIST", "LIST2") must be('empty)
        redis.lLen("LIST") must be(0)
        redis.lLen("LIST2") must be(0)
      }
    }
    "the source and dest keys are identical" should {
      "do nothing" taggedAs (V120) in {
        redis.rPush("LIST", "A")
        redis.rPopLPush("LIST", "LIST") must be(Some("A"))
        redis.lRange("LIST") must be(List("A"))
      }
    }
    "the dest key does not exist" should {
      "create a list at dest key and prepend the popped value" taggedAs (V120) in {
        redis.rPopLPush("LIST", "LIST2") must be(Some("A"))
        redis.lLen("LIST") must be(0)
        redis.lRange("LIST2") must be(List("A"))
      }
    }
    "one of the keys does not contain a list" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.rPopLPush("HASH", "LIST") } must produce[RedisCommandException]
        evaluating { redis.rPopLPush("LIST2", "HASH") } must produce[RedisCommandException]
      }
    }
    "both lists contain some elements" should {
      "pop the last element of list at source key and " +
        "prepend it to list at dest key" taggedAs (V120) in {
          redis.rPush("LIST", "A")
          redis.rPush("LIST", "B")
          redis.rPush("LIST", "C")

          redis.rPopLPush("LIST", "LIST2") must be(Some("C"))
          redis.lRange("LIST") must be(List("A", "B"))
          redis.lRange("LIST2") must be(List("C", "A"))

          redis.rPopLPush("LIST", "LIST2") must be(Some("B"))
          redis.lRange("LIST") must be(List("A"))
          redis.lRange("LIST2") must be(List("B", "C", "A"))

          redis.rPopLPush("LIST", "LIST2") must be(Some("A"))
          redis.lRange("LIST") must be('empty)
          redis.lRange("LIST2") must be(List("A", "B", "C", "A"))

          redis.rPopLPush("LIST2", "LIST") must be(Some("A"))
          redis.lRange("LIST") must be(List("A"))
          redis.lRange("LIST2") must be(List("A", "B", "C"))

          redis.del("LIST", "LIST2")
        }
    }
  }

  override def afterAll() {
    redis.flushDb().!
    redis.quit()
  }

}
