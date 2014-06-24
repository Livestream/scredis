package scredis.commands

import org.scalatest.{ ConfigMap, WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.{ Redis, Condition }
import scredis.exceptions.RedisCommandException
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

  Append when {
    "the key does not contain a string" should {
      "return an error" taggedAs (V200) in {
        an [RedisCommandException] must be thrownBy { redis.append("LIST", "a").! }
      }
    }
    "appending the empty string" should {
      Given("that the key does not exist")
      "create an empty string" taggedAs (V200) in {
        redis.append("NONEXISTENTKEY", "") must be(0)
        redis.get("NONEXISTENTKEY") must be(Some(""))
      }
      Given("that the key exists and contains a string")
      "not modify the current value" taggedAs (V200) in {
        redis.set("STR", "hello")
        redis.append("STR", "") must be(5)
        redis.get("STR") must be(Some("hello"))
      }
    }
    "appending some string" should {
      Given("that the key does not exist")
      "append (and create) the string and return the correct size" taggedAs (V200) in {
        redis.append("NONEXISTENTKEY", "hello") must be(5)
        redis.get("NONEXISTENTKEY") must be(Some("hello"))
        redis.del("NONEXISTENTKEY")
      }
      Given("that the key exists")
      "append the string and return the correct size" taggedAs (V200) in {
        redis.set("STR", "hello")
        redis.append("STR", "world") must be(10)
        redis.get("STR") must be(Some("helloworld"))
      }
    }
  }

  BitCount when {
    "the key does not exists" should {
      "return 0" taggedAs (V260) in {
        redis.bitCount("NONEXISTENTKEY") must be(0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V260) in {
        an [RedisCommandException] must be thrownBy { redis.bitCount("LIST").! }
      }
    }
    "the string is empty" should {
      "return 0" taggedAs (V260) in {
        redis.set("STR", "")
        redis.bitCount("STR") must be(0)
      }
    }
    "the string contains some bits set to 1" should {
      Given("that no interval is provided")
      "return the correct number of bits" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011
        redis.set("DOLLAR", "$")
        redis.set("POUND", "#")
        redis.bitCount("DOLLAR") must be(2)
        redis.bitCount("POUND") must be(3)
      }
      Given("that some interval is provided")
      "return the correct number of bits in the specified interval" taggedAs (V260) in {
        redis.set("BOTH", "$#")
        redis.bitCountInRange("BOTH", 0, 0) must be(2)
        redis.bitCountInRange("BOTH", 1, 1) must be(3)
        redis.bitCountInRange("BOTH", 0, 1) must be(5)
        redis.bitCountInRange("BOTH", -6, 42) must be(5)
      }
    }
  }

  BitOp when {
    "performing bitwise operations on keys that do not contain string values" should {
      "return an error" taggedAs (V260) in {
        an [RedisCommandException] must be thrownBy { redis.bitOpAnd("LIST", "A", "B").! }
        an [RedisCommandException] must be thrownBy { redis.bitOpOr("LIST", "A", "B").! }
        an [RedisCommandException] must be thrownBy { redis.bitOpXor("LIST", "A", "B").! }
        an [RedisCommandException] must be thrownBy { redis.bitOpNot("LIST", "B").! }
      }
    }
    "performing bitwise operations with non-existent keys" should {
      "return 0" taggedAs (V260) in {
        redis.bitOpAnd("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY") must be(0)
        redis.bitOpOr("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY") must be(0)
        redis.bitOpXor("NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY") must be(0)
        redis.bitOpNot("NONEXISTENTKEY", "NONEXISTENTKEY") must be(0)
      }
    }
    "performing bitwise operations with correct keys" should {
      "succeed and return the correct string sizes" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011

        redis.bitOpAnd("DOLLAR", "POUND", "AND") must be(1)
        redis.bitCount("AND") must be(1)

        redis.bitOpOr("DOLLAR", "POUND", "OR") must be(1)
        redis.bitCount("OR") must be(4)

        redis.bitOpXor("DOLLAR", "POUND", "XOR") must be(1)
        redis.bitCount("XOR") must be(3)

        redis.bitOpNot("DOLLAR", "NOT") must be(1)
        redis.bitCount("NOT") must be(6)
      }
    }
  }

  Decr when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.decr("LIST").! }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("DECR", "hello")
        an [RedisCommandException] must be thrownBy { redis.decr("DECR").! }
        redis.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        redis.decr("DECR") must be(-1)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        redis.decr("DECR") must be(-2)
      }
    }
  }

  DecrBy when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.decrBy("LIST", 1).! }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("DECR", "hello")
        an [RedisCommandException] must be thrownBy { redis.decrBy("DECR", 5).! }
        redis.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        redis.decrBy("DECR", 5) must be(-5)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        redis.decrBy("DECR", 5) must be(-10)
      }
    }
  }

  Get when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.get("NONEXISTENTKEY") must be('empty)
      }
    }
    "the key exists but sotred value is not a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.get("LIST").! }
      }
    }
    "the key contains a string" should {
      "return the string" taggedAs (V100) in {
        redis.set("STR", "VALUE")
        redis.get("STR") must be(Some("VALUE"))
      }
    }
  }

  GetBit when {
    "the key does not exist" should {
      "return false" taggedAs (V220) in {
        redis.getBit("NONEXISTENTKEY", 0) must be(false)
      }
    }
    "the key exists but the value is not a string" should {
      "return an error" taggedAs (V220) in {
        an [RedisCommandException] must be thrownBy { redis.getBit("LIST", 0).! }
      }
    }
    "the key exists but the offset is out of bound" should {
      "return an error" taggedAs (V220) in {
        redis.set("DOLLAR", "$")
        an [RedisCommandException] must be thrownBy { redis.getBit("DOLLAR", -1).! }
      }
    }
    "the key exists" should {
      "return the correct values" taggedAs (V220) in {
        // $ -> 00100100
        redis.getBit("DOLLAR", 0) must be(false)
        redis.getBit("DOLLAR", 1) must be(false)
        redis.getBit("DOLLAR", 2) must be(true)
        redis.getBit("DOLLAR", 3) must be(false)
        redis.getBit("DOLLAR", 4) must be(false)
        redis.getBit("DOLLAR", 5) must be(true)
        redis.getBit("DOLLAR", 6) must be(false)
        redis.getBit("DOLLAR", 7) must be(false)

        redis.getBit("DOLLAR", 8) must be(false)
        redis.getBit("DOLLAR", 9) must be(false)
      }
    }
  }

  Substr when {
    "the key does not exist" should {
      "return the empty string" taggedAs (V100) in {
        redis.substr("NONEXISTENTKEY", 0, 0) must be('empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.substr("LIST", 0, 0).! }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs (V100) in {
        redis.set("STR", "")
        redis.substr("STR", 0, 100) must be('empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs (V100) in {
        redis.set("STR", "01234")
        redis.substr("STR", 0, 100) must be("01234")
        redis.substr("STR", 0, 4) must be("01234")
        redis.substr("STR", 0, 2) must be("012")
        redis.substr("STR", 0, 0) must be("0")
        redis.substr("STR", 4, 4) must be("4")
        redis.substr("STR", 0, -1) must be("01234")
        redis.substr("STR", -3, -1) must be("234")
        redis.substr("STR", -1, -2) must be('empty)
      }
    }
  }

  GetRange when {
    "the key does not exist" should {
      "return the empty string" taggedAs (V240) in {
        redis.getRange("NONEXISTENTKEY", 0, 0) must be('empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V240) in {
        an [RedisCommandException] must be thrownBy { redis.getRange("LIST", 0, 0).! }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs (V240) in {
        redis.set("STR", "")
        redis.getRange("STR", 0, 100) must be('empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs (V240) in {
        redis.set("STR", "01234")
        redis.getRange("STR", 0, 100) must be("01234")
        redis.getRange("STR", 0, 4) must be("01234")
        redis.getRange("STR", 0, 2) must be("012")
        redis.getRange("STR", 0, 0) must be("0")
        redis.getRange("STR", 4, 4) must be("4")
        redis.getRange("STR", 0, -1) must be("01234")
        redis.getRange("STR", -3, -1) must be("234")
        redis.getRange("STR", -1, -2) must be('empty)
      }
    }
  }

  GetSet when {
    "the key does not exist" should {
      "return None and set the value" taggedAs (V100) in {
        redis.del("STR")
        redis.getSet("STR", SomeValue) must be('empty)
        redis.get("STR") must be(Some(SomeValue))
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.getSet("LIST", "A").! }
      }
    }
    "the key exists and has a string value" should {
      "return the current value and set the new value" taggedAs (V100) in {
        redis.getSet("STR", "YO") must be(Some(SomeValue))
        redis.get("STR") must be(Some("YO"))
      }
    }
  }

  Incr when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.incr("LIST").! }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("INCR", "hello")
        an [RedisCommandException] must be thrownBy { redis.incr("INCR").! }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        redis.incr("INCR") must be(1)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        redis.incr("INCR") must be(2)
      }
    }
  }

  IncrBy when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.incrBy("LIST", 1).! }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        redis.set("INCR", "hello")
        an [RedisCommandException] must be thrownBy { redis.incrBy("INCR", 5).! }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        redis.incrBy("INCR", 5) must be(5)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        redis.incrBy("INCR", 5) must be(10)
      }
    }
  }

  IncrByFloat when {
    "the value is not a string" should {
      "return an error" taggedAs (V260) in {
        an [RedisCommandException] must be thrownBy { redis.incrByFloat("LIST", 1.0).! }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V260) in {
        redis.set("INCR", "hello")
        an [RedisCommandException] must be thrownBy { redis.incrByFloat("INCR", 1.2).! }
        redis.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V260) in {
        redis.incrByFloat("INCR", 1.2) must be(1.2)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V260) in {
        redis.incrByFloat("INCR", 2.8) must be(4)
        redis.incrByFloat("INCR", 120e-2) must be(5.2)
      }
    }
  }

  MGet should {
    "return the value(s) associated to the given key(s)" taggedAs (V100) in {
      redis.mGet("NONEXISTENTKEY", "NONEXISTENTKEY2") must be(List(None, None))
      redis.mGetAsMap("NONEXISTENTKEY", "NONEXISTENTKEY2") must be('empty)

      redis.set("A", "a")
      redis.set("B", "")
      redis.set("C", "c")
      redis.mGet("A", "B", "C", "D", "LIST") must be(
        List(Some("a"), Some(""), Some("c"), None, None))
      redis.mGetAsMap("A", "B", "C", "D", "LIST") must be(Map("A" -> "a", "B" -> "", "C" -> "c"))
    }
  }

  MSet when {
    "setting keys that do not exist" should {
      "suceed" taggedAs (V101) in {
        redis.flushDb()
        redis.mSet(("A", "a"), ("B", ""), ("C", "c"))
        redis.mSetFromMap(Map("D" -> "d", "E" -> "", "F" -> "f"))
        redis.mGetAsMap("A", "B", "C", "D", "E", "F") must be(
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
    "setting keys that do already exist" should {
      "suceed & overwtite the previous values" taggedAs (V101) in {
        redis.mSet(("A", "A"), ("B", " "), ("C", "C"))
        redis.mSetFromMap(Map("D" -> "D", "E" -> " ", "F" -> "F"))
        redis.mGetAsMap("A", "B", "C", "D", "E", "F") must be(
          Map("A" -> "A", "B" -> " ", "C" -> "C", "D" -> "D", "E" -> " ", "F" -> "F"))
      }
    }
  }

  MSetNX when {
    "setting keys that do not exist" should {
      "succeed" taggedAs (V101) in {
        redis.flushDb()
        redis.mSetNX(("A", "a"), ("B", ""), ("C", "c")) must be(true)
        redis.mSetNXFromMap(Map("D" -> "d", "E" -> "", "F" -> "f")) must be(true)
        redis.mGetAsMap("A", "B", "C", "D", "E", "F") must be(
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
    "setting keys in which at least one of them does not exist" should {
      "do nothing" taggedAs (V101) in {
        redis.mSetNX(("A", "A"), ("B", " "), ("C", "C")) must be(false)
        redis.mSetNXFromMap(Map("D" -> "D", "E" -> " ", "F" -> "F")) must be(false)
        redis.mSetNX(("A", ""), ("X", "x"), ("Z", "z")) must be(false)
        redis.mGetAsMap("A", "B", "C", "D", "E", "F") must be(
          Map("A" -> "a", "B" -> "", "C" -> "c", "D" -> "d", "E" -> "", "F" -> "f"))
      }
    }
  }

  Set when {
    "setting a key that do not exist" should {
      "succeed" taggedAs (V100) in {
        redis.set("NONEXISTENTKEY", SomeValue)
        redis.get("NONEXISTENTKEY") must be(Some(SomeValue))
      }
    }
    "setting a key that already exists" should {
      "succeed" taggedAs (V100) in {
        redis.set("NONEXISTENTKEY", "A")
        redis.get("NONEXISTENTKEY") must be(Some("A"))
        redis.del("NONEXISTENTKEY")
      }
    }
    "setting a key of another type" should {
      "succeed" taggedAs (V100) in {
        redis.set("LIST", SomeValue)
        redis.get("LIST") must be(Some(SomeValue))
        redis.del("LIST")
        redis.lPush("LIST", "A")
      }
    }
    
    "setWithOptions" should {
      redis.del("KEY").!
      
      When("no options are provided")
      Given("the target key does not exist")
      "successfully set the value at key" taggedAs (V2612) in {
        redis.setWithOptions("KEY", SomeValue) must be(true)
        redis.get("KEY") must be(Some(SomeValue))
      }
      Given("the target key already exists")
      "successfully replace the value at key" taggedAs (V2612) in {
        redis.setWithOptions("KEY", "A") must be(true)
        redis.get("KEY") must be(Some("A"))
      }
      Given("the target key exists and is of another type")
      "successfully replace the value at key." taggedAs (V2612) in {
        redis.setWithOptions("LIST", SomeValue) must be(true)
        redis.get("LIST") must be(Some(SomeValue))
        redis.del("LIST")
        redis.lPush("LIST", "A")
      }
      
      redis.del("KEY").!
      When("expireAfter is provided")
      Given("the target key does not exist")
      "successfully set the value at key and expire it" taggedAs (V2612) in {
        redis.setWithOptions("KEY", SomeValue, expireAfter = Some(1 second)) must be(true)
        redis.get("KEY") must be(Some(SomeValue))
        redis.ttl("KEY") must be(Right(1))
        Thread.sleep(1000)
        redis.get("KEY") must be('empty)
      }
      
      When("NX condition is provided")
      Given("the target key does not exist")
      "successfully set the value at key." taggedAs (V2612) in {
        redis.setWithOptions("KEY", "A", condition = Some(Condition.IfDoesNotExist)) must be(true)
        redis.get("KEY") must be(Some("A"))
      }
      Given("the target key already exists")
      "return false" taggedAs (V2612) in {
        redis.setWithOptions("KEY", "B", condition = Some(Condition.IfDoesNotExist)) must be(false)
        redis.get("KEY") must be(Some("A"))
      }
      
      When("XX condition is provided")
      Given("the target key already exists")
      "successfully set the value at key.." taggedAs (V2612) in {
        redis.setWithOptions("KEY", "B", condition = Some(Condition.IfAlreadyExists)) must be(true)
        redis.get("KEY") must be(Some("B"))
      }
      Given("the target key does not exist")
      "return false." taggedAs (V2612) in {
        redis.del("KEY").!
        redis.setWithOptions("KEY", "C", condition = Some(Condition.IfAlreadyExists)) must be(false)
        redis.get("KEY") must be('empty)
      }
      
      When("both expireAfter and a condition are provided")
      "succeed" taggedAs (V2612) in {
        redis.setWithOptions(
          "KEY", "C", expireAfter = Some(1 second), condition = Some(Condition.IfDoesNotExist)
        ) must be(true)
        redis.get("KEY") must be(Some("C"))
        redis.ttl("KEY") must be(Right(1))
        Thread.sleep(1000)
        redis.get("KEY") must be('empty)
      }
    }
  }

  SetBit when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        redis.setBit("BIT", 0, true)
        redis.setBit("BIT", 3, true)
        redis.getBit("BIT", 0) must be(true)
        redis.getBit("BIT", 1) must be(false)
        redis.getBit("BIT", 2) must be(false)
        redis.getBit("BIT", 3) must be(true)
        redis.bitCount("BIT") must be(2)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        an [RedisCommandException] must be thrownBy { redis.setBit("LIST", 0, true).! }
      }
    }
    "the key exists" should {
      "succeed" taggedAs (V220) in {
        redis.setBit("BIT", 0, false)
        redis.setBit("BIT", 1, true)
        redis.setBit("BIT", 2, true)
        redis.getBit("BIT", 0) must be(false)
        redis.getBit("BIT", 1) must be(true)
        redis.getBit("BIT", 2) must be(true)
        redis.getBit("BIT", 3) must be(true)
        redis.bitCount("BIT") must be(3)
      }
    }
  }

  SetEX when {
    "the key does not exist" should {
      "succeed" taggedAs (V200) in {
        redis.setEX("TO-EXPIRE", SomeValue, 1)
        redis.get("TO-EXPIRE") must be(Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("TO-EXPIRE") must be(false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V200) in {
        redis.setEX("LIST", SomeValue, 1)
        redis.get("LIST") must be(Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("LIST") must be(false)
        redis.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V200) in {
        redis.set("TO-EXPIRE", SomeValue)
        redis.setEX("TO-EXPIRE", "Hello", 1)
        redis.get("TO-EXPIRE") must be(Some("Hello"))
        Thread.sleep(1050)
        redis.exists("TO-EXPIRE") must be(false)
      }
    }
  }

  PSetEX when {
    "the key does not exist" should {
      "succeed" taggedAs (V260) in {
        redis.pSetEX("TO-EXPIRE", SomeValue, 500)
        redis.get("TO-EXPIRE") must be(Some(SomeValue))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE") must be(false)

        redis.setEXDuration("TO-EXPIRE", SomeValue, 500 milliseconds)
        redis.get("TO-EXPIRE") must be(Some(SomeValue))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE") must be(false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V260) in {
        redis.pSetEX("LIST", SomeValue, 500)
        redis.get("LIST") must be(Some(SomeValue))
        Thread.sleep(550)
        redis.exists("LIST") must be(false)
        redis.lPush("LIST", "A")

        redis.setEXDuration("LIST", SomeValue, 500 milliseconds)
        redis.get("LIST") must be(Some(SomeValue))
        Thread.sleep(1050)
        redis.exists("LIST") must be(false)
        redis.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V260) in {
        redis.set("TO-EXPIRE", SomeValue)
        redis.pSetEX("TO-EXPIRE", "Hello", 500)
        redis.get("TO-EXPIRE") must be(Some("Hello"))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE") must be(false)

        redis.set("TO-EXPIRE", SomeValue)
        redis.setEXDuration("TO-EXPIRE", "Hello", 500 milliseconds)
        redis.get("TO-EXPIRE") must be(Some("Hello"))
        Thread.sleep(550)
        redis.exists("TO-EXPIRE") must be(false)
      }
    }
  }

  SetNX when {
    "the key does not exist" should {
      "succeed" taggedAs (V100) in {
        redis.setNX("NONEXISTENTKEY", SomeValue) must be(true)
        redis.get("NONEXISTENTKEY") must be(Some(SomeValue))
        redis.del("NONEXISTENTKEY")
      }
    }
    "the key already exists" should {
      "do nothing" taggedAs (V100) in {
        redis.set("IEXIST", "YEP")
        redis.setNX("IEXIST", SomeValue) must be(false)
        redis.get("IEXIST") must be(Some("YEP"))
        redis.setNX("LIST", "YEP") must be(false)
      }
    }
  }

  SetRange when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        redis.del("STR")
        redis.setRange("STR", 5, "YES")
        redis.exists("STR") must be(true)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        an [RedisCommandException] must be thrownBy { redis.setRange("LIST", 5, "YES").! }
      }
    }
    "the key already exists" should {
      "succeed" taggedAs (V220) in {
        redis.setRange("STR", 0, "HELLO")
        redis.get("STR") must be(Some("HELLOYES"))
      }
    }
  }

  StrLen when {
    "the key does not exist" should {
      "return 0" taggedAs (V220) in {
        redis.del("STR")
        redis.strLen("STR") must be(0)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        an [RedisCommandException] must be thrownBy { redis.strLen("LIST").! }
      }
    }
    "the key contains a string" should {
      "succeed and return the correct length" taggedAs (V220) in {
        redis.set("STR", "")
        redis.strLen("STR") must be(0)
        redis.set("STR", "Hello")
        redis.strLen("STR") must be(5)
      }
    }
  }

  override def afterAll(configMap: ConfigMap) {
    redis.flushAll().!
    redis.quit()
  }

}
