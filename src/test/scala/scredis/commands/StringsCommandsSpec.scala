package scredis.commands

import org.scalatest.{ ConfigMap, WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.{ Redis, Condition }
import scredis.exceptions.RedisErrorResponseException
import scredis.tags._

import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._

class StringsCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll() {
    redis.lPush("LIST", "A")
  }

  import Names._
  import scredis.util.TestUtils._
  import redis.ec

  Append.name when {
    "the key does not contain a string" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.append("LIST", "a").futureValue }
      }
    }
    "appending the empty string" should {
      Given("that the key does not exist")
      "create an empty string" taggedAs (V200) in {
        redis.append("NONEXISTENTKEY", "").futureValue should be (0)
        redis.get("NONEXISTENTKEY").futureValue should be (Some(""))
      }
      Given("that the key exists and contains a string")
      "not modify the current value" taggedAs (V200) in {
        redis.set("STR", "hello")
        redis.append("STR", "").futureValue should be (5)
        redis.get("STR").futureValue should be (Some("hello"))
      }
    }
    "appending some string" should {
      Given("that the key does not exist")
      "append (and create) the string and return the correct size" taggedAs (V200) in {
        redis.append("NONEXISTENTKEY", "hello").futureValue should be (5)
        redis.get("NONEXISTENTKEY").futureValue should be (Some("hello"))
        redis.del("NONEXISTENTKEY")
      }
      Given("that the key exists")
      "append the string and return the correct size" taggedAs (V200) in {
        redis.set("STR", "hello")
        redis.append("STR", "world").futureValue should be (10)
        redis.get("STR").futureValue should be (Some("helloworld"))
      }
    }
  }

  BitCount.name when {
    "the key does not exists" should {
      "return 0" taggedAs (V260) in {
        redis.bitCount("NONEXISTENTKEY").futureValue should be (0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { redis.bitCount("LIST").futureValue }
      }
    }
    "the string is empty" should {
      "return 0" taggedAs (V260) in {
        redis.set("STR", "")
        redis.bitCount("STR").futureValue should be (0)
      }
    }
    "the string contains some bits set to 1" should {
      Given("that no interval is provided")
      "return the correct number of bits" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011
        redis.set("DOLLAR", "$")
        redis.set("POUND", "#")
        redis.bitCount("DOLLAR").futureValue should be (2)
        redis.bitCount("POUND").futureValue should be (3)
      }
      Given("that some interval is provided")
      "return the correct number of bits in the specified interval" taggedAs (V260) in {
        redis.set("BOTH", "$#")
        redis.bitCountInRange("BOTH", 0, 0).futureValue should be (2)
        redis.bitCountInRange("BOTH", 1, 1).futureValue should be (3)
        redis.bitCountInRange("BOTH", 0, 1).futureValue should be (5)
        redis.bitCountInRange("BOTH", -6, 42).futureValue should be (5)
      }
    }
  }

  BitOp.name when {
    "performing bitwise operations on keys that do not contain string values" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { redis.bitOpAnd("LIST", "A", "B").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.bitOpOr("LIST", "A", "B").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.bitOpXor("LIST", "A", "B").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.bitOpNot("LIST", "B").futureValue }
      }
    }
    "performing bitwise operations with non-existent keys" should {
      "return 0" taggedAs (V260) in {
        redis.bitOpAnd("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY").futureValue should be (0)
        redis.bitOpOr("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY").futureValue should be (0)
        redis.bitOpXor("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY").futureValue should be (0)
        redis.bitOpNot("NONEXISTENTKEY", "NONEXISTENTKEY").futureValue should be (0)
      }
    }
    "performing bitwise operations with correct keys" should {
      "succeed and return the correct string sizes" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011

        redis.bitOpAnd("DOLLAR", "POUND", "AND").futureValue should be (1)
        redis.bitCount("AND").futureValue should be (1)

        redis.bitOpOr("DOLLAR", "POUND", "OR").futureValue should be (1)
        redis.bitCount("OR").futureValue should be (4)

        redis.bitOpXor("DOLLAR", "POUND", "XOR").futureValue should be (1)
        redis.bitCount("XOR").futureValue should be (3)

        redis.bitOpNot("DOLLAR", "NOT").futureValue should be (1)
        redis.bitCount("NOT").futureValue should be (6)
      }
    }
  }

  Decr.name when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.decr("LIST").futureValue }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("DECR", "hello")
        a [RedisErrorResponseException] should be thrownBy { redis.decr("DECR").futureValue }
        redis.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        redis.decr("DECR").futureValue should be (-1)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        redis.decr("DECR").futureValue should be (-2)
      }
    }
  }

  DecrBy.name when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.decrBy("LIST", 1).futureValue }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("DECR", "hello")
        a [RedisErrorResponseException] should be thrownBy { redis.decrBy("DECR", 5).futureValue }
        redis.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        redis.decrBy("DECR", 5).futureValue should be (-5)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        redis.decrBy("DECR", 5).futureValue should be (-10)
      }
    }
  }

  Get.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.get("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key exists but sotred value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.get("LIST").futureValue }
      }
    }
    "the key contains a string" should {
      "return the string" taggedAs (V100) in {
        redis.set("STR", "VALUE")
        redis.get("STR").futureValue should be (Some("VALUE"))
      }
    }
  }

  GetBit.name when {
    "the key does not exist" should {
      "return false" taggedAs (V220) in {
        redis.getBit("NONEXISTENTKEY", 0).futureValue should be (false)
      }
    }
    "the key exists but the value is not a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { redis.getBit("LIST", 0).futureValue }
      }
    }
    "the key exists but the offset is out of bound" should {
      "return an error" taggedAs (V220) in {
        redis.set("DOLLAR", "$")
        a [RedisErrorResponseException] should be thrownBy { redis.getBit("DOLLAR", -1).futureValue }
      }
    }
    "the key exists" should {
      "return the correct values" taggedAs (V220) in {
        // $ -> 00100100
        redis.getBit("DOLLAR", 0).futureValue should be (false)
        redis.getBit("DOLLAR", 1).futureValue should be (false)
        redis.getBit("DOLLAR", 2).futureValue should be (true)
        redis.getBit("DOLLAR", 3).futureValue should be (false)
        redis.getBit("DOLLAR", 4).futureValue should be (false)
        redis.getBit("DOLLAR", 5).futureValue should be (true)
        redis.getBit("DOLLAR", 6).futureValue should be (false)
        redis.getBit("DOLLAR", 7).futureValue should be (false)

        redis.getBit("DOLLAR", 8).futureValue should be (false)
        redis.getBit("DOLLAR", 9).futureValue should be (false)
      }
    }
  }

  Substr.name when {
    "the key does not exist" should {
      "return the empty string" taggedAs (V100) in {
        redis.substr("NONEXISTENTKEY", 0, 0).futureValue should be (empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.substr("LIST", 0, 0).futureValue }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs (V100) in {
        redis.set("STR", "")
        redis.substr("STR", 0, 100).futureValue should be (empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs (V100) in {
        redis.set("STR", "01234")
        redis.substr("STR", 0, 100).futureValue should be ("01234")
        redis.substr("STR", 0, 4).futureValue should be ("01234")
        redis.substr("STR", 0, 2).futureValue should be ("012")
        redis.substr("STR", 0, 0).futureValue should be ("0")
        redis.substr("STR", 4, 4).futureValue should be ("4")
        redis.substr("STR", 0, -1).futureValue should be ("01234")
        redis.substr("STR", -3, -1).futureValue should be ("234")
        redis.substr("STR", -1, -2).futureValue should be (empty)
      }
    }
  }

  GetRange.name when {
    "the key does not exist" should {
      "return the empty string" taggedAs (V240) in {
        redis.getRange("NONEXISTENTKEY", 0, 0).futureValue should be (empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy { redis.getRange("LIST", 0, 0).futureValue }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs (V240) in {
        redis.set("STR", "")
        redis.getRange("STR", 0, 100).futureValue should be (empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs (V240) in {
        redis.set("STR", "01234")
        redis.getRange("STR", 0, 100).futureValue should be ("01234")
        redis.getRange("STR", 0, 4).futureValue should be ("01234")
        redis.getRange("STR", 0, 2).futureValue should be ("012")
        redis.getRange("STR", 0, 0).futureValue should be ("0")
        redis.getRange("STR", 4, 4).futureValue should be ("4")
        redis.getRange("STR", 0, -1).futureValue should be ("01234")
        redis.getRange("STR", -3, -1).futureValue should be ("234")
        redis.getRange("STR", -1, -2).futureValue should be (empty)
      }
    }
  }

  GetSet.name when {
    "the key does not exist" should {
      "return None and set the value" taggedAs (V100) in {
        redis.del("STR")
        redis.getSet("STR", SomeValue).futureValue should be (empty)
        redis.get("STR").futureValue should be (Some(SomeValue))
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.getSet("LIST", "A").futureValue }
      }
    }
    "the key exists and has a string value" should {
      "return the current value and set the new value" taggedAs (V100) in {
        redis.getSet("STR", "YO").futureValue should be (Some(SomeValue))
        redis.get("STR").futureValue should be (Some("YO"))
      }
    }
  }

  Incr.name when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.incr("LIST").futureValue }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy { redis.incr("INCR").futureValue }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        redis.incr("INCR").futureValue should be (1)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        redis.incr("INCR").futureValue should be (2)
      }
    }
  }

  IncrBy.name when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.incrBy("LIST", 1).futureValue }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy { redis.incrBy("INCR", 5).futureValue }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        redis.incrBy("INCR", 5).futureValue should be (5)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        redis.incrBy("INCR", 5).futureValue should be (10)
      }
    }
  }

  IncrByFloat.name when {
    "the value is not a string" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { redis.incrByFloat("LIST", 1.0).futureValue }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V260) in {
        redis.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy { redis.incrByFloat("INCR", 1.2).futureValue }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V260) in {
        redis.incrByFloat("INCR", 1.2).futureValue should be (1.2)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V260) in {
        redis.incrByFloat("INCR", 2.8).futureValue should be (4)
        redis.incrByFloat("INCR", 120e-2).futureValue should be (5.2)
      }
    }
  }

  MGet should {
    "return the value(s) associated to the given key(s)" taggedAs (V100) in {
      redis.mGet("NONEXISTENTKEY", "NONEXISTENTKEY2").futureValue should be (List(None, None))
      redis.mGetAsMap("NONEXISTENTKEY", "NONEXISTENTKEY2").futureValue should be (empty)

      redis.set("A", "a")
      redis.set("B", "")
      redis.set("C", "c")
      redis.mGet("A", "B", "C", "D", "LIST").futureValue should be (
        List(Some("a"), Some(""), Some("c"), None, None))
      redis.mGetAsMap("A", "B", "C", "D", "LIST").futureValue should be (Map("A" -> "a", "B" -> "", "C" -> "c"))
    }
  }

  MSet.name when {
    "setting keys that do not exist" should {
      "suceed" taggedAs (V101) in {
        redis.flushDb()
        redis.mSet(("A", "a"), ("B", ""), ("C", "c"))
        redis.mSetFromMap(Map("D" -> "d", "E" -> "", "F" -> "f"))
        redis.mGetAsMap("A", "B", "C", "D", "E", "F").futureValue should be (
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
    "setting keys that do already exist" should {
      "suceed & overwtite the previous values" taggedAs (V101) in {
        redis.mSet(("A", "A"), ("B", " "), ("C", "C"))
        redis.mSetFromMap(Map("D" -> "D", "E" -> " ", "F" -> "F"))
        redis.mGetAsMap("A", "B", "C", "D", "E", "F").futureValue should be (
          Map("A" -> "A", "B" -> " ", "C" -> "C", "D" -> "D", "E" -> " ", "F" -> "F"))
      }
    }
  }

  MSetNX.name when {
    "setting keys that do not exist" should {
      "succeed" taggedAs (V101) in {
        redis.flushDb()
        redis.mSetNX(("A", "a"), ("B", ""), ("C", "c")).futureValue should be (true)
        redis.mSetNXFromMap(Map("D" -> "d", "E" -> "", "F" -> "f")).futureValue should be (true)
        redis.mGetAsMap("A", "B", "C", "D", "E", "F").futureValue should be (
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
    "setting keys in which at least one of them does not exist" should {
      "do nothing" taggedAs (V101) in {
        redis.mSetNX(("A", "A"), ("B", " "), ("C", "C")).futureValue should be (false)
        redis.mSetNXFromMap(Map("D" -> "D", "E" -> " ", "F" -> "F")).futureValue should be (false)
        redis.mSetNX(("A", ""), ("X", "x"), ("Z", "z")).futureValue should be (false)
        redis.mGetAsMap("A", "B", "C", "D", "E", "F").futureValue should be (
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
  }

  Set.name when {
    "setting a key that do not exist" should {
      "succeed" taggedAs (V100) in {
        redis.set("NONEXISTENTKEY", SomeValue)
        redis.get("NONEXISTENTKEY").futureValue should be (Some(SomeValue))
      }
    }
    "setting a key that already exists" should {
      "succeed" taggedAs (V100) in {
        redis.set("NONEXISTENTKEY", "A")
        redis.get("NONEXISTENTKEY").futureValue should be (Some("A"))
        redis.del("NONEXISTENTKEY")
      }
    }
    "setting a key of another type" should {
      "succeed" taggedAs (V100) in {
        redis.set("LIST", SomeValue)
        redis.get("LIST").futureValue should be (Some(SomeValue))
        redis.del("LIST")
        redis.lPush("LIST", "A")
      }
    }
    
    "setWithOptions" should {
      redis.del("KEY").futureValue
      
      When("no options are provided")
      Given("the target key does not exist")
      "successfully set the value at key" taggedAs (V2612) in {
        redis.setWithOptions("KEY", SomeValue).futureValue should be (true)
        redis.get("KEY").futureValue should be (Some(SomeValue))
      }
      Given("the target key already exists")
      "successfully replace the value at key" taggedAs (V2612) in {
        redis.setWithOptions("KEY", "A").futureValue should be (true)
        redis.get("KEY").futureValue should be (Some("A"))
      }
      Given("the target key exists and is of another type")
      "successfully replace the value at key." taggedAs (V2612) in {
        redis.setWithOptions("LIST", SomeValue).futureValue should be (true)
        redis.get("LIST").futureValue should be (Some(SomeValue))
        redis.del("LIST")
        redis.lPush("LIST", "A")
      }
      
      redis.del("KEY").futureValue
      When("expireAfter is provided")
      Given("the target key does not exist")
      "successfully set the value at key and expire it" taggedAs (V2612) in {
        redis.setWithOptions("KEY", SomeValue, expireAfter = Some(1 second)).futureValue should be (true)
        redis.get("KEY").futureValue should be (Some(SomeValue))
        redis.ttl("KEY").futureValue should be (Right(1))
        Thread.sleep(1000)
        redis.get("KEY").futureValue should be (empty)
      }
      
      When("NX condition is provided")
      Given("the target key does not exist")
      "successfully set the value at key." taggedAs (V2612) in {
        redis.setWithOptions("KEY", "A", condition = Some(Condition.IfDoesNotExist)).futureValue should be (true)
        redis.get("KEY").futureValue should be (Some("A"))
      }
      Given("the target key already exists")
      "return false" taggedAs (V2612) in {
        redis.setWithOptions("KEY", "B", condition = Some(Condition.IfDoesNotExist)).futureValue should be (false)
        redis.get("KEY").futureValue should be (Some("A"))
      }
      
      When("XX condition is provided")
      Given("the target key already exists")
      "successfully set the value at key.." taggedAs (V2612) in {
        redis.setWithOptions("KEY", "B", condition = Some(Condition.IfAlreadyExists)).futureValue should be (true)
        redis.get("KEY").futureValue should be (Some("B"))
      }
      Given("the target key does not exist")
      "return false." taggedAs (V2612) in {
        redis.del("KEY").futureValue
        redis.setWithOptions("KEY", "C", condition = Some(Condition.IfAlreadyExists)).futureValue should be (false)
        redis.get("KEY").futureValue should be (empty)
      }
      
      When("both expireAfter and a condition are provided")
      "succeed" taggedAs (V2612) in {
        redis.setWithOptions(
          "KEY", "C", expireAfter = Some(1 second), condition = Some(Condition.IfDoesNotExist)
        ).futureValue should be (true)
        redis.get("KEY").futureValue should be (Some("C"))
        redis.ttl("KEY").futureValue should be (Right(1))
        Thread.sleep(1000)
        redis.get("KEY").futureValue should be (empty)
      }
    }
  }

  SetBit.name when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        redis.setBit("BIT", 0, true)
        redis.setBit("BIT", 3, true)
        redis.getBit("BIT", 0).futureValue should be (true)
        redis.getBit("BIT", 1).futureValue should be (false)
        redis.getBit("BIT", 2).futureValue should be (false)
        redis.getBit("BIT", 3).futureValue should be (true)
        redis.bitCount("BIT").futureValue should be (2)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { redis.setBit("LIST", 0, true).futureValue }
      }
    }
    "the key exists" should {
      "succeed" taggedAs (V220) in {
        redis.setBit("BIT", 0, false)
        redis.setBit("BIT", 1, true)
        redis.setBit("BIT", 2, true)
        redis.getBit("BIT", 0).futureValue should be (false)
        redis.getBit("BIT", 1).futureValue should be (true)
        redis.getBit("BIT", 2).futureValue should be (true)
        redis.getBit("BIT", 3).futureValue should be (true)
        redis.bitCount("BIT").futureValue should be (3)
      }
    }
  }

  SetEX.name when {
    "the key does not exist" should {
      "succeed" taggedAs (V200) in {
        redis.setEX("TO-EXPIRE", SomeValue, 1)
        redis.get("TO-EXPIRE").futureValue should be (Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V200) in {
        redis.setEX("LIST", SomeValue, 1)
        redis.get("LIST").futureValue should be (Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("LIST").futureValue should be (false)
        redis.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V200) in {
        redis.set("TO-EXPIRE", SomeValue)
        redis.setEX("TO-EXPIRE", "Hello", 1)
        redis.get("TO-EXPIRE").futureValue should be (Some("Hello"))
        Thread.sleep(1050)
        redis.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
  }

  PSetEX.name when {
    "the key does not exist" should {
      "succeed" taggedAs (V260) in {
        redis.pSetEX("TO-EXPIRE", SomeValue, 500)
        redis.get("TO-EXPIRE").futureValue should be (Some(SomeValue))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE").futureValue should be (false)

        redis.setEXDuration("TO-EXPIRE", SomeValue, 500 milliseconds)
        redis.get("TO-EXPIRE").futureValue should be (Some(SomeValue))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V260) in {
        redis.pSetEX("LIST", SomeValue, 500)
        redis.get("LIST").futureValue should be (Some(SomeValue))
        Thread.sleep(550)
        redis.exists("LIST").futureValue should be (false)
        redis.lPush("LIST", "A")

        redis.setEXDuration("LIST", SomeValue, 500 milliseconds)
        redis.get("LIST").futureValue should be (Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("LIST").futureValue should be (false)
        redis.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V260) in {
        redis.set("TO-EXPIRE", SomeValue)
        redis.pSetEX("TO-EXPIRE", "Hello", 500)
        redis.get("TO-EXPIRE").futureValue should be (Some("Hello"))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE").futureValue should be (false)

        redis.set("TO-EXPIRE", SomeValue)
        redis.setEXDuration("TO-EXPIRE", "Hello", 500 milliseconds)
        redis.get("TO-EXPIRE").futureValue should be (Some("Hello"))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
  }

  SetNX.name when {
    "the key does not exist" should {
      "succeed" taggedAs (V100) in {
        redis.setNX("NONEXISTENTKEY", SomeValue).futureValue should be (true)
        redis.get("NONEXISTENTKEY").futureValue should be (Some(SomeValue))
        redis.del("NONEXISTENTKEY")
      }
    }
    "the key already exists" should {
      "do nothing" taggedAs (V100) in {
        redis.set("IEXIST", "YEP")
        redis.setNX("IEXIST", SomeValue).futureValue should be (false)
        redis.get("IEXIST").futureValue should be (Some("YEP"))
        redis.setNX("LIST", "YEP").futureValue should be (false)
      }
    }
  }

  SetRange.name when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        redis.del("STR")
        redis.setRange("STR", 5, "YES")
        redis.exists("STR").futureValue should be (true)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { redis.setRange("LIST", 5, "YES").futureValue }
      }
    }
    "the key already exists" should {
      "succeed" taggedAs (V220) in {
        redis.setRange("STR", 0, "HELLO")
        redis.get("STR").futureValue should be (Some("HELLOYES"))
      }
    }
  }

  StrLen.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V220) in {
        redis.del("STR")
        redis.strLen("STR").futureValue should be (0)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { redis.strLen("LIST").futureValue }
      }
    }
    "the key contains a string" should {
      "succeed and return the correct length" taggedAs (V220) in {
        redis.set("STR", "")
        redis.strLen("STR").futureValue should be (0)
        redis.set("STR", "Hello")
        redis.strLen("STR").futureValue should be (5)
      }
    }
  }

  override def afterAll(configMap: ConfigMap) {
    redis.flushAll().futureValue
    redis.quit()
  }

}
