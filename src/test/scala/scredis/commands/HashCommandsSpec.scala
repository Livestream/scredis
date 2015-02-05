package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.HashRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer

class HashCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll() = {
    client.lPush("LIST", "A").futureValue
  }
  
  HDel.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.hDel("NONEXISTENTKEY", "foo").futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hDel("LIST", "foo").!
        }
      }
    }
    "the hash does not contain the field" should {
      "return 0" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", SomeValue)
        client.hDel("HASH", "FIELD2").futureValue should be (0)
      }
    }
    "the hash contains the field" should {
      "return 1" taggedAs (V200) in {
        client.hDel("HASH", "FIELD").futureValue should be (1)
        client.hExists("HASH", "FIELD").futureValue should be (false)
      }
    }
  }

  s"${HDel.toString} >= 2.4" when {
    "the key does not exist" should {
      "return 0" taggedAs (V240) in {
        client.hDel("NONEXISTENTKEY", "foo", "bar").futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hDel("LIST", "foo", "bar").!
        }
      }
    }
    "the hash does not contain the fields" should {
      "return 0" taggedAs (V240) in {
        client.hSet("HASH", "FIELD", SomeValue)
        client.hSet("HASH", "FIELD2", SomeValue)
        client.hSet("HASH", "FIELD3", SomeValue)
        client.hDel("HASH", "FIELD4", "FIELD5").futureValue should be (0)
      }
    }
    "the hash contains the fields" should {
      "return the correct number of deleted fields" taggedAs (V240) in {
        client.hDel("HASH", "FIELD", "FIELD4", "FIELD5").futureValue should be (1)
        client.hDel("HASH", "FIELD2", "FIELD3").futureValue should be (2)
        client.hExists("HASH", "FIELD").futureValue should be (false)
        client.hExists("HASH", "FIELD2").futureValue should be (false)
        client.hExists("HASH", "FIELD3").futureValue should be (false)
      }
    }
  }

  HExists.toString when {
    "the key does not exist" should {
      "return false" taggedAs (V200) in {
        client.hExists("NONEXISTENTKEY", "foo").futureValue should be (false)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hExists("LIST", "foo").!
        }
      }
    }
    "the hash does not contain the field" should {
      "return false" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", SomeValue)
        client.hExists("HASH", "FIELD2").futureValue should be (false)
      }
    }
    "the hash contains the field" should {
      "return true" taggedAs (V200) in {
        client.hExists("HASH", "FIELD").futureValue should be (true)
        client.hDel("HASH", "FIELD").futureValue should be (1)
      }
    }
  }

  HGet.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hGet("NONEXISTENTKEY", "foo").futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hGet("LIST", "foo").!
        }
      }
    }
    "the hash does not contain the field" should {
      "return None" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", SomeValue)
        client.hGet("HASH", "FIELD2").futureValue should be (empty)
      }
    }
    "the hash contains the field" should {
      "return the stored value" taggedAs (V200) in {
        client.hGet("HASH", "FIELD").futureValue should contain (SomeValue)
        client.hDel("HASH", "FIELD").futureValue should be (1)
      }
    }
  }

  HGetAll.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hGetAll("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hGetAll("LIST").!
        }
      }
    }
    "the hash contains some fields" should {
      "return the stored value" taggedAs (V200) in {
        client.hSet("HASH", "FIELD1", SomeValue)
        client.hSet("HASH", "FIELD2", "YES")
        client.hGetAll("HASH").futureValue should contain (
          Map("FIELD1" -> SomeValue, "FIELD2" -> "YES")
        )
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HIncrBy.toString when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs (V200) in {
        client.hIncrBy("HASH", "FIELD", 1).futureValue should be (1)
        client.hGet("HASH", "FIELD").futureValue should contain ("1")
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hIncrBy("LIST", "foo", 1).!
        }
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs (V200) in {
        client.hIncrBy("HASH", "FIELD2", 3).futureValue should be (3)
        client.hGet("HASH", "FIELD2").futureValue should contain ("3")
      }
    }
    "the hash contains the field but the latter does not contain an integer" should {
      "return an error" taggedAs (V200) in {
        client.hSet("HASH", "FIELD3", SomeValue)
        a [RedisErrorResponseException] should be thrownBy {
          client.hIncrBy("HASH", "FIELD3", 2).!
        }
      }
    }
    "the hash contains the field and the latter is an integer" should {
      "increment the value" taggedAs (V200) in {
        client.hIncrBy("HASH", "FIELD", -3).futureValue should be (-2)
        client.hGet("HASH", "FIELD").futureValue should contain ("-2")
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HIncrByFloat.toString when {
    "the key does not exist" should {
      "create a hash with the specified field and increment the field" taggedAs (V260) in {
        client.hIncrByFloat("HASH", "FIELD", 1.2).futureValue should be (1.2)
        client.hGet("HASH", "FIELD").futureValue should contain ("1.2")
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hIncrByFloat("LIST", "foo", 1.2).!
        }
      }
    }
    "the hash does not contain the field" should {
      "create the field and increment it" taggedAs (V260) in {
        client.hIncrByFloat("HASH", "FIELD2", 3.4).futureValue should be (3.4)
        client.hGet("HASH", "FIELD2").futureValue should contain ("3.4")
      }
    }
    "the hash contains the field but the latter does not contain a floating point number" should {
      "return an error" taggedAs (V260) in {
        client.hSet("HASH", "FIELD3", SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          client.hIncrByFloat("HASH", "FIELD3", 2.1).!
        }
      }
    }
    "the hash contains the field and the latter is a floating point number" should {
      "increment the value" taggedAs (V260) in {
        client.hIncrByFloat("HASH", "FIELD", -3.1).futureValue should be (-1.9)
        client.hGet("HASH", "FIELD").futureValue should contain ("-1.9")
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HKeys.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hKeys("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { client.hKeys("LIST").! }
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs (V200) in {
        client.hSet("HASH", "FIELD1", SomeValue)
        client.hSet("HASH", "FIELD2", "YES")
        client.hKeys("HASH").futureValue should contain theSameElementsAs List("FIELD1", "FIELD2")
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.hLen("NONEXISTENTKEY").futureValue should be (0)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { client.hLen("LIST").! }
      }
    }
    "the hash contains some fields" should {
      "return the number of fields in the hash" taggedAs (V200) in {
        client.hSet("HASH", "FIELD1", SomeValue)
        client.hSet("HASH", "FIELD2", "YES")
        client.hLen("HASH").futureValue should be (2)
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HMGet.toString when {
    "the key does not exist" should {
      "return a list contianing only None" taggedAs (V200) in {
        client.hmGet("NONEXISTENTKEY", "foo", "bar").futureValue should contain theSameElementsAs (
          List(None, None)
        )
        client.hmGetAsMap("NONEXISTENTKEY", "foo", "bar").futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hmGet("LIST", "foo", "bar").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.hmGetAsMap("LIST", "foo", "bar").!
        }
      }
    }
    "the hash contains some fields" should {
      "return the values" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", SomeValue)
        client.hSet("HASH", "FIELD2", "YES")
        client.hmGet(
          "HASH", "FIELD", "FIELD2", "FIELD3"
        ).futureValue should contain theSameElementsAs (List(Some(SomeValue), Some("YES"), None))
        client.hmGetAsMap("HASH", "FIELD", "FIELD2", "FIELD3").futureValue should be (
          Map("FIELD" -> SomeValue, "FIELD2" -> "YES")
        )
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HMSet.toString when {
    "the key does not exist" should {
      "create a hash and set all specified fields" taggedAs (V200) in {
        client.hmSet(
          "HASH", Map("FIELD" -> SomeValue, "FIELD2" -> "YES")
        ).futureValue should be (())
        client.hmGet("HASH", "FIELD", "FIELD2").futureValue should contain theSameElementsAs (
          List(Some(SomeValue), Some("YES"))
        )
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.hmSet("LIST", Map("FIELD" -> SomeValue, "FIELD2" -> "YES")).!
        }
      }
    }
    "the hash contains some fields" should {
      "set and overwrite fields" taggedAs (V200) in {
        client.hmSet(
          "HASH", Map("FIELD" -> "NO", "FIELD2" -> "", "FIELD3" -> "YES")
        ).futureValue should be (())
        client.hmGet("HASH", "FIELD", "FIELD2", "FIELD3").futureValue should be (
          List(Some("NO"), Some(""), Some("YES"))
        )
        client.del("HASH").futureValue should be (1)
      }
    }
  }
  
  HScan.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, list) = client.hScan[String, String]("NONEXISTENTKEY", 0).!
        next should be (0)
        list should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hScan[String, String]("LIST", 0).!
        }
      }
    }
    "the hash contains 5 fields" should {
      "return all fields" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.hSet("HASH", "key" + i, "value" + i)
        }
        val (next, list) = client.hScan[String, String]("HASH", 0).!
        next should be (0)
        list should contain theSameElementsAs List(
          ("key1", "value1"),
          ("key2", "value2"),
          ("key3", "value3"),
          ("key4", "value4"),
          ("key5", "value5")
        )
        for (i <- 1 to 10) {
          client.hSet("HASH", "foo" + i, "foo" + i)
        }
      }
    }
    "the hash contains 15 fields" should {
      val all = ListBuffer[(String, String)]()
      for (i <- 1 to 5) {
        all += (("key" + i, "value" + i))
      }
      for (i <- 1 to 10) {
        all += (("foo" + i, "foo" + i))
      }
      
      Given("that no pattern is set")
      "return all fields" taggedAs (V280) in {
        val elements = ListBuffer[(String, String)]()
        var cursor = 0L
        do {
          val (next, list) = client.hScan[String, String]("HASH", cursor).!
          elements ++= list
          cursor = next
        }
        while (cursor > 0)
        elements.toList should contain theSameElementsAs all
      }
      Given("that a pattern is set")
      "return all matching fields" taggedAs (V280) in {
        val elements = ListBuffer[(String, String)]()
        var cursor = 0L
        do {
          val (next, list) = client.hScan[String, String](
            "HASH", cursor, matchOpt = Some("foo*")
          ).!
          elements ++= list
          cursor = next
        }
        while (cursor > 0)
        elements.toList should contain theSameElementsAs (all.filter {
          case (key, value) => key.startsWith("foo")
        })
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching fields in one iteration" taggedAs (V280) in {
        val elements = ListBuffer[(String, String)]()
        var cursor = 0L
        do {
          val (next, list) = client.hScan[String, String](
            "HASH", cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          list should have size (10)
          elements ++= list
          cursor = next
        }
        while (cursor > 0)
        elements.toList should contain theSameElementsAs (all.filter {
          case (key, value) => key.startsWith("foo")
        })
      }
    }
  }

  HSet.toString when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", SomeValue).futureValue should be (true)
        client.hGet("HASH", "FIELD").futureValue should contain (SomeValue)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hSet("LIST", "foo", "bar").!
        }
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs (V200) in {
        client.hSet("HASH", "FIELD2", "YES").futureValue should be (true)
        client.hGet("HASH", "FIELD2").futureValue should contain ("YES")
      }
    }
    "the hash already contains the field" should {
      "overwrite the value" taggedAs (V200) in {
        client.hSet("HASH", "FIELD", "NEW").futureValue should be (false)
        client.hGet("HASH", "FIELD").futureValue should contain ("NEW")
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HSetNX.toString when {
    "the key does not exist" should {
      "create the hash and set the given field" taggedAs (V200) in {
        client.hSetNX("HASH", "FIELD", SomeValue).futureValue should be (true)
        client.hGet("HASH", "FIELD").futureValue should contain (SomeValue)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hSetNX("LIST", "foo", "bar").!
        }
      }
    }
    "the hash already exists and does not contain the field" should {
      "set the new field" taggedAs (V200) in {
        client.hSetNX("HASH", "FIELD2", "YES").futureValue should be (true)
        client.hGet("HASH", "FIELD2").futureValue should contain ("YES")
      }
    }
    "the hash already contains the field" should {
      "return an error" taggedAs (V200) in {
        client.hSetNX("HASH", "FIELD", "NEW").futureValue should be (false)
        client.hGet("HASH", "FIELD").futureValue should contain (SomeValue)
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  HVals.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.hVals("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key does not contain a hash" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.hVals("LIST").!
        }
      }
    }
    "the hash contains some fields" should {
      "return field names" taggedAs (V200) in {
        client.hSet("HASH", "FIELD1", SomeValue)
        client.hSet("HASH", "FIELD2", "YES")
        client.hSet("HASH", "FIELD3", "YES")
        client.hVals("HASH").futureValue should contain theSameElementsAs (
          List(SomeValue, "YES", "YES")
        )
        client.del("HASH").futureValue should be (1)
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
