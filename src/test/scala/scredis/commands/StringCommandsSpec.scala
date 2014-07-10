package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.StringRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.duration._

class StringCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll() {
    client.lPush("LIST", "A")
  }

  Append.toString when {
    "the key does not contain a string" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.append("LIST", "a").!
        }
      }
    }
    "appending the empty string" should {
      Given("that the key does not exist")
      "create an empty string" taggedAs (V200) in {
        client.append("NONEXISTENTKEY", "").futureValue should be (0)
        client.get("NONEXISTENTKEY").futureValue should contain ("")
      }
      Given("that the key exists and contains a string")
      "not modify the current value" taggedAs (V200) in {
        client.set("STR", "hello")
        client.append("STR", "").futureValue should be (5)
        client.get("STR").futureValue should contain ("hello")
      }
    }
    "appending some string" should {
      Given("that the key does not exist")
      "append (and create) the string and return the correct size" taggedAs (V200) in {
        client.append("NONEXISTENTKEY", "hello").futureValue should be (5)
        client.get("NONEXISTENTKEY").futureValue should contain ("hello")
        client.del("NONEXISTENTKEY")
      }
      Given("that the key exists")
      "append the string and return the correct size" taggedAs (V200) in {
        client.set("STR", "hello")
        client.append("STR", "world").futureValue should be (10)
        client.get("STR").futureValue should contain ("helloworld")
      }
    }
  }

  BitCount.toString when {
    "the key does not exists" should {
      "return 0" taggedAs (V260) in {
        client.bitCount("NONEXISTENTKEY").futureValue should be (0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.bitCount("LIST").!
        }
      }
    }
    "the string is empty" should {
      "return 0" taggedAs (V260) in {
        client.set("STR", "")
        client.bitCount("STR").futureValue should be (0)
      }
    }
    "the string contains some bits set to 1" should {
      Given("that no interval is provided")
      "return the correct number of bits" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011
        client.set("DOLLAR", "$")
        client.set("POUND", "#")
        client.bitCount("DOLLAR").futureValue should be (2)
        client.bitCount("POUND").futureValue should be (3)
      }
      Given("that some interval is provided")
      "return the correct number of bits in the specified interval" taggedAs (V260) in {
        client.set("BOTH", "$#")
        client.bitCount("BOTH", 0, 0).futureValue should be (2)
        client.bitCount("BOTH", 1, 1).futureValue should be (3)
        client.bitCount("BOTH", 0, 1).futureValue should be (5)
        client.bitCount("BOTH", -6, 42).futureValue should be (5)
      }
    }
  }

  scredis.protocol.requests.StringRequests.BitOp.toString when {
    "performing bitwise operations on keys that do not contain string values" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.bitOp(scredis.BitOp.And, "LIST", "A", "B").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.bitOp(scredis.BitOp.Or, "LIST", "A", "B").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.bitOp(scredis.BitOp.Xor, "LIST", "A", "B").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.bitOp(scredis.BitOp.Not, "LIST", "B").!
        }
      }
    }
    "performing bitwise operations with non-existent keys" should {
      "return 0" taggedAs (V260) in {
        client.bitOp(
          scredis.BitOp.And, "NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY"
        ).futureValue should be (0)
        client.bitOp(
          scredis.BitOp.Or, "NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY"
        ).futureValue should be (0)
        client.bitOp(
          scredis.BitOp.Xor, "NONEXISTENTKEY", "NONEXISTENTKEY", "NONEXISTENTKEY"
        ).futureValue should be (0)
        client.bitOp(
          scredis.BitOp.Not, "NONEXISTENTKEY", "NONEXISTENTKEY"
        ).futureValue should be (0)
      }
    }
    "performing bitwise operations with correct keys" should {
      "succeed and return the correct string sizes" taggedAs (V260) in {
        // $ -> 00100100
        // # -> 00100011

        client.bitOp(scredis.BitOp.And, "DOLLAR", "POUND", "AND").futureValue should be (1)
        client.bitCount("AND").futureValue should be (1)

        client.bitOp(scredis.BitOp.Or, "DOLLAR", "POUND", "OR").futureValue should be (1)
        client.bitCount("OR").futureValue should be (4)

        client.bitOp(scredis.BitOp.Xor, "DOLLAR", "POUND", "XOR").futureValue should be (1)
        client.bitCount("XOR").futureValue should be (3)

        client.bitOp(scredis.BitOp.Not, "DOLLAR", "NOT").futureValue should be (1)
        client.bitCount("NOT").futureValue should be (6)
      }
    }
  }
  
  BitPos.toString when {
    "the key does not exists" should {
      "return 0" taggedAs (V287) in {
        client.bitPos("NONEXISTENTKEY", true).futureValue should be (0)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V287) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.bitCount("LIST").!
        }
      }
    }
    "the key contains $" should {
      "return the position of the first bit set/unset" taggedAs (V287) in {
        client.set("STRING", "\\xff\\xf0\\x00")
        client.bitPos("STRING", false).futureValue should be (12)
        client.bitPos("STRING", true).futureValue should be (0)
        
        client.bitPos("STRING", false, 1).futureValue should be (4)
        client.bitPos("STRING", true, 2).futureValue should be (-1)
        client.bitPos("STRING", true, 0, 1).futureValue should be (0)
        client.bitPos("STRING", false, 3).futureValue should be (24)
        client.bitPos("STRING", false, 0, 0).futureValue should be (-1)
        
        client.del("STRING")
      }
    }
  }

  Decr.toString when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.decr("LIST").!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        client.set("DECR", "hello")
        a [RedisErrorResponseException] should be thrownBy {
          client.decr("DECR").!
        }
        client.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        client.decr("DECR").futureValue should be (-1)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        client.decr("DECR").futureValue should be (-2)
      }
    }
  }

  DecrBy.toString when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.decrBy("LIST", 1).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        client.set("DECR", "hello")
        a [RedisErrorResponseException] should be thrownBy {
          client.decrBy("DECR", 5).!
        }
        client.del("DECR")
      }
    }
    "the key does not exist" should {
      "decrement from zero" taggedAs (V100) in {
        client.decrBy("DECR", 5).futureValue should be (-5)
      }
    }
    "the key exists" should {
      "decrement from current value" taggedAs (V100) in {
        client.decrBy("DECR", 5).futureValue should be (-10)
      }
    }
  }

  Get.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.get("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key exists but sotred value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.get("LIST").!
        }
      }
    }
    "the key contains a string" should {
      "return the string" taggedAs (V100) in {
        client.set("STR", "VALUE")
        client.get("STR").futureValue should contain ("VALUE")
      }
    }
  }

  GetBit.toString when {
    "the key does not exist" should {
      "return false" taggedAs (V220) in {
        client.getBit("NONEXISTENTKEY", 0).futureValue should be (false)
      }
    }
    "the key exists but the value is not a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.getBit("LIST", 0).!
        }
      }
    }
    "the key exists but the offset is out of bound" should {
      "return an error" taggedAs (V220) in {
        client.set("DOLLAR", "$")
        a [RedisErrorResponseException] should be thrownBy {
          client.getBit("DOLLAR", -1).!
        }
      }
    }
    "the key exists" should {
      "return the correct values" taggedAs (V220) in {
        // $ -> 00100100
        client.getBit("DOLLAR", 0).futureValue should be (false)
        client.getBit("DOLLAR", 1).futureValue should be (false)
        client.getBit("DOLLAR", 2).futureValue should be (true)
        client.getBit("DOLLAR", 3).futureValue should be (false)
        client.getBit("DOLLAR", 4).futureValue should be (false)
        client.getBit("DOLLAR", 5).futureValue should be (true)
        client.getBit("DOLLAR", 6).futureValue should be (false)
        client.getBit("DOLLAR", 7).futureValue should be (false)

        client.getBit("DOLLAR", 8).futureValue should be (false)
        client.getBit("DOLLAR", 9).futureValue should be (false)
      }
    }
  }
  
  GetRange.toString when {
    "the key does not exist" should {
      "return the empty string" taggedAs (V240) in {
        client.getRange("NONEXISTENTKEY", 0, 0).futureValue should be (empty)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.getRange("LIST", 0, 0).!
        }
      }
    }
    "the key exists and contains an empty string" should {
      "return the empty string" taggedAs (V240) in {
        client.set("STR", "")
        client.getRange("STR", 0, 100).futureValue should be (empty)
      }
    }
    "the key exists and contains a non-empty string" should {
      "return the correct substring" taggedAs (V240) in {
        client.set("STR", "01234")
        client.getRange("STR", 0, 100).futureValue should be ("01234")
        client.getRange("STR", 0, 4).futureValue should be ("01234")
        client.getRange("STR", 0, 2).futureValue should be ("012")
        client.getRange("STR", 0, 0).futureValue should be ("0")
        client.getRange("STR", 4, 4).futureValue should be ("4")
        client.getRange("STR", 0, -1).futureValue should be ("01234")
        client.getRange("STR", -3, -1).futureValue should be ("234")
        client.getRange("STR", -1, -2).futureValue should be (empty)
      }
    }
  }

  GetSet.toString when {
    "the key does not exist" should {
      "return None and set the value" taggedAs (V100) in {
        client.del("STR")
        client.getSet("STR", SomeValue).futureValue should be (empty)
        client.get("STR").futureValue should contain (SomeValue)
      }
    }
    "the key exists but does not contain a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.getSet("LIST", "A").!
        }
      }
    }
    "the key exists and has a string value" should {
      "return the current value and set the new value" taggedAs (V100) in {
        client.getSet("STR", "YO").futureValue should contain (SomeValue)
        client.get("STR").futureValue should contain ("YO")
      }
    }
  }

  Incr.toString when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.incr("LIST").!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        client.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy {
          client.incr("INCR").!
        }
        client.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        client.incr("INCR").futureValue should be (1)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        client.incr("INCR").futureValue should be (2)
      }
    }
  }

  IncrBy.toString when {
    "the value is not a string" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.incrBy("LIST", 1).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V100) in {
        client.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy {
          client.incrBy("INCR", 5).!
        }
        client.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V100) in {
        client.incrBy("INCR", 5).futureValue should be (5)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V100) in {
        client.incrBy("INCR", 5).futureValue should be (10)
      }
    }
  }

  IncrByFloat.toString when {
    "the value is not a string" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.incrByFloat("LIST", 1.0).!
        }
      }
    }
    "the value is a string but is not an integer value" should {
      "return an error" taggedAs (V260) in {
        client.set("INCR", "hello")
        a [RedisErrorResponseException] should be thrownBy {
          client.incrByFloat("INCR", 1.2).!
        }
        client.del("INCR")
      }
    }
    "the key does not exist" should {
      "increment from zero" taggedAs (V260) in {
        client.incrByFloat("INCR", 1.2).futureValue should be (1.2)
      }
    }
    "the key exists" should {
      "increment from current value" taggedAs (V260) in {
        client.incrByFloat("INCR", 2.8).futureValue should be (4)
        client.incrByFloat("INCR", 120e-2).futureValue should be (5.2)
      }
    }
  }

  MGet.toString should {
    "return the value(s) associated to the given key(s)" taggedAs (V100) in {
      client.mGet(
        "NONEXISTENTKEY", "NONEXISTENTKEY2"
      ).futureValue should contain theSameElementsInOrderAs List(None, None)
      client.mGetAsMap("NONEXISTENTKEY", "NONEXISTENTKEY2").futureValue should be (empty)

      client.set("A", "a")
      client.set("B", "")
      client.set("C", "c")
      client.mGet("A", "B", "C", "D", "LIST").futureValue should be (
        List(Some("a"), Some(""), Some("c"), None, None))
      client.mGetAsMap(
        "A", "B", "C", "D", "LIST"
      ).futureValue should contain theSameElementsAs List(
        ("A", "a"), ("B", ""), ("C", "c")
      )
    }
  }

  MSet.toString when {
    "setting keys that do not exist" should {
      "suceed" taggedAs (V101) in {
        client.flushDB()
        client.mSet(Map("A" -> "a", "B" -> "", "C" -> "c"))
        client.mSet(Map("D" -> "d", "E" -> "", "F" -> "f"))
        client.mGetAsMap(
          "A", "B", "C", "D", "E", "F"
        ).futureValue should contain theSameElementsAs List(
          ("A", "a"), ("B" -> ""), ("C" -> "c"), ("D", "d"), ("E", ""), ("F", "f")
        )
      }
    }
    "setting keys that do already exist" should {
      "suceed & overwtite the previous values" taggedAs (V101) in {
        client.mSet(Map("A" -> "A", "B" -> " ", "C" -> "C"))
        client.mSet(Map("D" -> "D", "E" -> " ", "F" -> "F"))
        client.mGetAsMap(
          "A", "B", "C", "D", "E", "F"
        ).futureValue should contain theSameElementsAs List(
          ("A", "A"), ("B", " "), ("C", "C"), ("D", "D"), ("E", " "), ("F", "F")
        )
      }
    }
  }

  MSetNX.toString when {
    "setting keys that do not exist" should {
      "succeed" taggedAs (V101) in {
        client.flushDB()
        client.mSetNX(Map("A" -> "a", "B" -> "", "C" -> "c")).futureValue should be (true)
        client.mSetNX(Map("D" -> "d", "E" -> "", "F" -> "f")).futureValue should be (true)
        client.mGetAsMap(
          "A", "B", "C", "D", "E", "F"
        ).futureValue should contain theSameElementsAs List(
          ("A", "a"), ("B", ""), ("C", "c"), ("D", "d"), ("E", ""), ("F", "f")
        )
      }
    }
    "setting keys in which at least one of them does not exist" should {
      "do nothing" taggedAs (V101) in {
        client.mSetNX(Map("A" -> "A", "B" -> " ", "C" -> "C")).futureValue should be (false)
        client.mSetNX(Map("D" -> "D", "E" -> " ", "F" -> "F")).futureValue should be (false)
        client.mSetNX(Map("A" -> "", "X" -> "x", "Z" -> "z")).futureValue should be (false)
        client.mGetAsMap(
          "A", "B", "C", "D", "E", "F"
        ).futureValue should contain theSameElementsAs List(
          ("A", "a"), ("B", ""), ("C", "c"), ("D", "d"), ("E", ""), ("F", "f")
        )
      }
    }
  }
  
  PSetEX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V260) in {
        client.pSetEX("TO-EXPIRE", SomeValue, 500)
        client.get("TO-EXPIRE").futureValue should contain (SomeValue)
        Thread.sleep(550)
        client.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V260) in {
        client.pSetEX("LIST", SomeValue, 500)
        client.get("LIST").futureValue should contain (SomeValue)
        Thread.sleep(550)
        client.exists("LIST").futureValue should be (false)
        client.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V260) in {
        client.set("TO-EXPIRE", SomeValue)
        client.pSetEX("TO-EXPIRE", "Hello", 500)
        client.get("TO-EXPIRE").futureValue should contain ("Hello")
        Thread.sleep(550)
        client.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
  }

  Set.toString when {
    "setting a key that do not exist" should {
      "succeed" taggedAs (V100) in {
        client.set("NONEXISTENTKEY", SomeValue)
        client.get("NONEXISTENTKEY").futureValue should contain (SomeValue)
      }
    }
    "setting a key that already exists" should {
      "succeed" taggedAs (V100) in {
        client.set("NONEXISTENTKEY", "A")
        client.get("NONEXISTENTKEY").futureValue should contain ("A")
        client.del("NONEXISTENTKEY")
      }
    }
    "setting a key of another type" should {
      "succeed" taggedAs (V100) in {
        client.set("LIST", SomeValue)
        client.get("LIST").futureValue should contain (SomeValue)
        client.del("LIST")
        client.lPush("LIST", "A")
      }
    }
    "setWithOptions" should {
      client.del("KEY")
      
      When("expireAfter is provided")
      Given("the target key does not exist")
      "successfully set the value at key and expire it" taggedAs (V2612) in {
        client.set("KEY", SomeValue, ttlOpt = Some(500 milliseconds)).futureValue should be (true)
        client.get("KEY").futureValue should contain (SomeValue)
        client.ttl("KEY").futureValue should be (Right(1))
        Thread.sleep(550)
        client.get("KEY").futureValue should be (empty)
      }
      
      When("NX condition is provided")
      Given("the target key does not exist")
      "successfully set the value at key." taggedAs (V2612) in {
        client.set("KEY", "A", conditionOpt = Some(Condition.NX)).futureValue should be (true)
        client.get("KEY").futureValue should contain ("A")
      }
      Given("the target key already exists")
      "return false" taggedAs (V2612) in {
        client.set("KEY", "B", conditionOpt = Some(Condition.NX)).futureValue should be (false)
        client.get("KEY").futureValue should contain ("A")
      }
      
      When("XX condition is provided")
      Given("the target key already exists")
      "successfully set the value at key.." taggedAs (V2612) in {
        client.set("KEY", "B", conditionOpt = Some(Condition.XX)).futureValue should be (true)
        client.get("KEY").futureValue should contain ("B")
      }
      Given("the target key does not exist")
      "return false." taggedAs (V2612) in {
        client.del("KEY").futureValue
        client.set("KEY", "C", conditionOpt = Some(Condition.XX)).futureValue should be (false)
        client.get("KEY").futureValue should be (empty)
      }
      
      When("both expireAfter and a condition are provided")
      "succeed" taggedAs (V2612) in {
        client.set(
          "KEY", "C", ttlOpt = Some(500 milliseconds), conditionOpt = Some(Condition.NX)
        ).futureValue should be (true)
        client.get("KEY").futureValue should contain ("C")
        client.ttl("KEY").futureValue should be (Right(1))
        Thread.sleep(550)
        client.get("KEY").futureValue should be (empty)
      }
    }
  }

  SetBit.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        client.setBit("BIT", 0, true)
        client.setBit("BIT", 3, true)
        client.getBit("BIT", 0).futureValue should be (true)
        client.getBit("BIT", 1).futureValue should be (false)
        client.getBit("BIT", 2).futureValue should be (false)
        client.getBit("BIT", 3).futureValue should be (true)
        client.bitCount("BIT").futureValue should be (2)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.setBit("LIST", 0, true).!
        }
      }
    }
    "the key exists" should {
      "succeed" taggedAs (V220) in {
        client.setBit("BIT", 0, false)
        client.setBit("BIT", 1, true)
        client.setBit("BIT", 2, true)
        client.getBit("BIT", 0).futureValue should be (false)
        client.getBit("BIT", 1).futureValue should be (true)
        client.getBit("BIT", 2).futureValue should be (true)
        client.getBit("BIT", 3).futureValue should be (true)
        client.bitCount("BIT").futureValue should be (3)
      }
    }
  }

  SetEX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V200) in {
        client.setEX("TO-EXPIRE", SomeValue, 1)
        client.get("TO-EXPIRE").futureValue should contain (SomeValue)
        Thread.sleep(1050)
        client.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
    "the key does not contain a string" should {
      "succeed" taggedAs (V200) in {
        client.setEX("LIST", SomeValue, 1)
        client.get("LIST").futureValue should contain (SomeValue)
        Thread.sleep(1050)
        client.exists("LIST").futureValue should be (false)
        client.lPush("LIST", "A")
      }
    }
    "the key already exists" should {
      "succeed and ovewrite the previous value" taggedAs (V200) in {
        client.set("TO-EXPIRE", SomeValue)
        client.setEX("TO-EXPIRE", "Hello", 1)
        client.get("TO-EXPIRE").futureValue should contain ("Hello")
        Thread.sleep(1050)
        client.exists("TO-EXPIRE").futureValue should be (false)
      }
    }
  }

  SetNX.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V100) in {
        client.setNX("NONEXISTENTKEY", SomeValue).futureValue should be (true)
        client.get("NONEXISTENTKEY").futureValue should contain (SomeValue)
        client.del("NONEXISTENTKEY")
      }
    }
    "the key already exists" should {
      "do nothing" taggedAs (V100) in {
        client.set("IEXIST", "YEP")
        client.setNX("IEXIST", SomeValue).futureValue should be (false)
        client.get("IEXIST").futureValue should contain ("YEP")
        client.setNX("LIST", "YEP").futureValue should be (false)
      }
    }
  }

  SetRange.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        client.del("STR")
        client.setRange("STR", 5, "YES")
        client.exists("STR").futureValue should be (true)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.setRange("LIST", 5, "YES").!
        }
      }
    }
    "the key already exists" should {
      "succeed" taggedAs (V220) in {
        client.setRange("STR", 0, "HELLO")
        client.get("STR").futureValue should contain ("HELLOYES")
      }
    }
  }

  StrLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V220) in {
        client.del("STR")
        client.strLen("STR").futureValue should be (0)
      }
    }
    "the key does not contain a string" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.strLen("LIST").!
        }
      }
    }
    "the key contains a string" should {
      "succeed and return the correct length" taggedAs (V220) in {
        client.set("STR", "")
        client.strLen("STR").futureValue should be (0)
        client.set("STR", "Hello")
        client.strLen("STR").futureValue should be (5)
      }
    }
  }

  override def afterAll(configMap: ConfigMap) {
    client.flushAll().!
    client.quit().!
  }

}
