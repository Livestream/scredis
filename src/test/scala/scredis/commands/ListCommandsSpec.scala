package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.ListRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class ListCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    client.hSet("HASH", "FIELD", SomeValue).!
  }
  
  /* FIXME:
  BLPop.toString when {
    "the first existing key is not of type list" should {
      "return an error" taggedAs (V200) in {
        client.rPush("LIST", "A").futureValue
        a [RedisErrorResponseException] should be thrownBy {
          client.blPop(1, "HASH", "LIST")
        }
        client.del("LIST").!
        a [RedisErrorResponseException] should be thrownBy  {
          client.blPop(1, "LIST", "HASH")
        }
      }
    }
    "the keys do not exist or are empty" should {
      "succeed" taggedAs (V200) in {
        client.blPop(1, "LIST", "LIST2", "LIST3").futureValue should be (empty)
      }
    }
    "the timeout is zero" should {
      "block and return as soon as a value is pushed" taggedAs (V200) in {
        val future = Future {
          val start = System.currentTimeMillis
          val result = client.sync(_.blPop(0, "LIST", "LIST2", "LIST3"))
          val elapsed = System.currentTimeMillis - start
          (result, elapsed)
        }
        Thread.sleep(500)
        client.rPush("LIST2", "A").futureValue
        val (result, elapsed) = Await.result(future, 50 milliseconds)
        result.get should be (("LIST2" -> "A"))
        elapsed.toInt should be >= (500)
      }
    }
    "the keys contain a non-empty list" should {
      "pop the values in order" taggedAs (V200) in {
        client.rPush("LIST", "A").futureValue
        client.rPush("LIST", "B").futureValue
        client.rPush("LIST2", "C").futureValue
        client.rPush("LIST3", "D").futureValue
        client.rPush("LIST3", "E").futureValue
        client.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get should be (("LIST" -> "A"))
        client.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get should be (("LIST" -> "B"))
        client.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get should be (("LIST2" -> "C"))
        client.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get should be (("LIST3" -> "D"))
        client.sync(_.blPop(1, "LIST", "LIST2", "LIST3")).get should be (("LIST3" -> "E"))
      }
    }
    "the BLPOP timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BLPOP timeout" taggedAs (V200) in {
        client.sync { client =>
          client.setTimeout(200 milliseconds)
          client.blPop(1, "LIST", "LIST2", "LIST3").futureValue should be (empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }

  BRPop.toString when {
    "the first existing key is not of type list" should {
      "return an error" taggedAs (V200) in {
        client.lPush("LIST", "A").futureValue
        a [RedisErrorResponseException] should be thrownBy {
          client.sync(_.brPop(1, "HASH", "LIST"))
        }
        client.del("LIST").futureValue
        a [RedisErrorResponseException] should be thrownBy {
          client.sync(_.brPop(1, "LIST", "HASH"))
        }
      }
    }
    "the keys do not exist or are empty" should {
      "succeed" taggedAs (V200) in {
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3")).futureValue should be (empty)
      }
    }
    "the timeout is zero" should {
      "block and return as soon as a value is pushed" taggedAs (V200) in {
        val future = Future {
          val start = System.currentTimeMillis
          val result = client.sync(_.brPop(0, "LIST", "LIST2", "LIST3"))
          val elapsed = System.currentTimeMillis - start
          (result, elapsed)
        }
        Thread.sleep(500)
        client.lPush("LIST2", "A").futureValue
        val (result, elapsed) = Await.result(future, 50 milliseconds)
        result.get should be (("LIST2" -> "A"))
        elapsed.toInt should be >= (500)
      }
    }
    "the keys contain a non-empty list" should {
      "return the values in order" taggedAs (V200) in {
        client.lPush("LIST", "A").futureValue
        client.lPush("LIST", "B").futureValue
        client.lPush("LIST2", "C").futureValue
        client.lPush("LIST3", "D").futureValue
        client.lPush("LIST3", "E").futureValue
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get should be (("LIST" -> "A")))
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get should be (("LIST" -> "B")))
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get should be (("LIST2" -> "C")))
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get should be (("LIST3" -> "D")))
        client.sync(_.brPop(1, "LIST", "LIST2", "LIST3").get should be (("LIST3" -> "E")))
      }
    }
    "the BRPOP timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BRPOP timeout" taggedAs (V200) in {
        client.sync { client =>
          client.setTimeout(200 milliseconds)
          client.brPop(1, "LIST", "LIST2", "LIST3").futureValue should be (empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }

  BRPopLPush.toString when {
    "the source key does not exist" should {
      "do nothing" taggedAs (V220) in {
        client.sync(_.brPopLPush("LIST", "LIST", 1)).futureValue should be (empty)
      }
    }
    "the source key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sync(_.brPopLPush("HASH", "LIST", 1))
        }
      }
    }
    "the dest key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        client.lPush("LIST", "A").futureValue
        a [RedisErrorResponseException] should be thrownBy {
          client.sync(_.brPopLPush("LIST", "HASH", 1))
        }
      }
    }
    "the source and dest keys are correct" should {
      "succeed" taggedAs (V220) in {
        client.sync(_.brPopLPush("LIST", "LIST", 1)).futureValue should be (Some("A"))
        client.sync(_.brPopLPush("LIST", "LIST2", 1)).futureValue should be (Some("A"))
        client.lPop("LIST").futureValue should be (empty)
        client.lPop("LIST2").futureValue should be (Some("A"))
      }
    }
    "the BRPopLPush timeout is greater than the default timeout" should {
      "adjust the connection timeout to the BRPopLPush timeout" taggedAs (V200) in {
        client.sync { client =>
          client.setTimeout(200 milliseconds)
          client.brPopLPush("LIST", "LIST", 1).futureValue should be (empty)
          client.restoreDefaultTimeout()
        }
      }
    }
  }*/

  LIndex.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.lIndex("LIST", 0).futureValue should be (empty)
        client.lIndex("LIST", 100).futureValue should be (empty)
        client.lIndex("LIST", -1).futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { client.lIndex("HASH", 0).! }
      }
    }
    "the index is out of range" should {
      "return None" taggedAs (V100) in {
        client.rPush("LIST", "Hello")
        client.rPush("LIST", "World")
        client.rPush("LIST", "!")
        client.lIndex("LIST", -100).futureValue should be (empty)
        client.lIndex("LIST", 100).futureValue should be (empty)
      }
    }
    "the index is correct" should {
      "return the value stored at index" taggedAs (V100) in {
        client.lIndex("LIST", 0).futureValue should contain ("Hello")
        client.lIndex("LIST", 1).futureValue should contain ("World")
        client.lIndex("LIST", 2).futureValue should contain ("!")

        client.lIndex("LIST", -1).futureValue should contain ("!")
        client.lIndex("LIST", -2).futureValue should contain ("World")
        client.lIndex("LIST", -3).futureValue should contain ("Hello")
        client.del("LIST")
      }
    }
  }

  LInsert.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        client.lInsert("LIST", Position.Before, "A", SomeValue).futureValue should contain (0)
        client.lPop("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lInsert("HASH", Position.Before, "A", SomeValue).!
        }
      }
    }
    "the pivot is not in the list" should {
      "return None" taggedAs (V220) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "C")
        client.rPush("LIST", "E")
        client.lInsert("LIST", Position.Before, "X", SomeValue).futureValue should be (empty)
      }
    }
    "the pivot is in the list" should {
      "insert the element at the correct position" taggedAs (V220) in {
        client.lInsert("LIST", Position.After, "A", "B").futureValue should contain (4)
        client.lInsert("LIST", Position.Before, "E", "D").futureValue should contain (5)
        client.lInsert("LIST", Position.After, "E", "G").futureValue should contain (6)
        client.lInsert("LIST", Position.Before, "G", "F").futureValue should contain (7)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "E", "F", "G"
        )
        client.del("LIST")
      }
    }
  }

  LLen.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        client.lLen("LIST").futureValue should be (0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { client.lLen("HASH").! }
      }
    }
    "the list contains some elements" should {
      "return the number of elements contained in the list" taggedAs (V100) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.lLen("LIST").futureValue should be (2)
        client.del("LIST")
      }
    }
  }

  LPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.lPop("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { client.lPop("HASH").! }
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs (V100) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.lPop("LIST").futureValue should contain ("A")
        client.lPop("LIST").futureValue should contain ("B")
        client.lPop("LIST").futureValue should be (empty)
      }
    }
  }

  LPush.toString when {
    "the key does not exist" should {
      "create a list and prepend the value" taggedAs (V100) in {
        client.lPush("LIST", "A").futureValue should be (1)
        client.lPush("LIST", "B").futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("B", "A")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lPush("HASH", "A").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs (V100) in {
        client.lPush("LIST", "1").futureValue should be (3)
        client.lPush("LIST", "2").futureValue should be (4)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "2", "1", "B", "A"
        )
        client.del("LIST")
      }
    }
  }

  s"${LPush.toString}-2.4" when {
    "the key does not exist" should {
      "create a list and prepend the values" taggedAs (V240) in {
        client.lPush("LIST", "A", "B").futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("B", "A")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lPush("HASH", "A", "B").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the values to the existing list" taggedAs (V240) in {
        client.lPush("LIST", "1", "2").futureValue should be (4)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "2", "1", "B", "A"
        )
        client.del("LIST")
      }
    }
  }

  LPushX.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        client.lPushX("LIST", "A").futureValue should be (0)
        client.lPushX("LIST", "B").futureValue should be (0)
        client.lRange("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lPushX("HASH", "A").!
        }
      }
    }
    "the list contains some elements" should {
      "prepend the value to the existing list" taggedAs (V220) in {
        client.lPush("LIST", "3")
        client.lPushX("LIST", "2").futureValue should be (2)
        client.lPushX("LIST", "1").futureValue should be (3)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "1", "2", "3"
        )
        client.del("LIST")
      }
    }
  }

  LRange.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.lRange("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lRange("HASH").!
        }
      }
    }
    "the list contains some elements" should {
      "return the elements in the specified range" taggedAs (V100) in {
        client.rPush("LIST", "0")
        client.rPush("LIST", "1")
        client.rPush("LIST", "2")
        client.rPush("LIST", "3")
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        client.lRange("LIST", 0, -1).futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        client.lRange("LIST", 0, 0).futureValue should contain theSameElementsInOrderAs List("0")
        client.lRange("LIST", 1, 1).futureValue should contain theSameElementsInOrderAs List("1")
        client.lRange("LIST", 2, 2).futureValue should contain theSameElementsInOrderAs List("2")
        client.lRange("LIST", 3, 3).futureValue should contain theSameElementsInOrderAs List("3")
        client.lRange("LIST", 1, 2).futureValue should contain theSameElementsInOrderAs List(
          "1", "2"
        )
        client.lRange("LIST", -2, -1).futureValue should contain theSameElementsInOrderAs List(
          "2", "3"
        )
        client.lRange("LIST", -1, -1).futureValue should contain theSameElementsInOrderAs List("3")
        client.lRange("LIST", -4, -1).futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        client.del("LIST")
      }
    }
  }

  LRem.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        client.lRem("LIST", "A").futureValue should be (0)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lRem("HASH", "A").!
        }
      }
    }
    "the list contains some elements" should {
      "delete the specified elements" taggedAs (V100) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.rPush("LIST", "C")
        client.rPush("LIST", "D")
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.rPush("LIST", "C")
        client.rPush("LIST", "D")

        client.lRem("LIST", "X").futureValue should be (0)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "A", "B", "C", "D"
        )

        client.lRem("LIST", "A").futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D", "B", "C", "D"
        )

        client.lRem("LIST", "B", 1).futureValue should be (1)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "C", "D", "B", "C", "D"
        )

        client.lRem("LIST", "C", -1).futureValue should be (1)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "C", "D", "B", "D"
        )

        client.lRem("LIST", "D", -2).futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("C", "B")

        client.lRem("LIST", "C", 50).futureValue should be (1)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("B")

        client.lRem("LIST", "B", -50).futureValue should be (1)
        client.lRange("LIST").futureValue should be (empty)
      }
    }
  }

  LSet.toString when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", 0, "A").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", 1, "A").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", -1, "A").!
        }
        client.lRange("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("HASH", 0, "A").!
        }
      }
    }
    "the list contains some elements" should {
      Given("the index is out of range")
      "return an error" taggedAs (V100) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.rPush("LIST", "C")
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", 3, "X").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", -4, "X").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.lSet("LIST", 55, "X").!
        }
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
      }
      Given("the index is correct")
      "set the provided values at the corresponding indices" taggedAs (V100) in {
        client.lSet("LIST", 0, "D")
        client.lSet("LIST", 1, "E")
        client.lSet("LIST", 2, "F")
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "D", "E", "F"
        )
        client.lSet("LIST", -3, "A")
        client.lSet("LIST", -2, "B")
        client.lSet("LIST", -1, "C")
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
        client.del("LIST")
      }
    }
  }

  LTrim.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs (V100) in {
        client.lTrim("LIST", 0, -1)
        client.lRange("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.lTrim("HASH", 0, -1).!
        }
      }
    }
    "the list contains some elements" should {
      "trim the list to the specified range" taggedAs (V100) in {
        client.rPush("LIST", "0")
        client.rPush("LIST", "1")
        client.rPush("LIST", "2")
        client.rPush("LIST", "3")

        client.rPush("LIST2", "0")
        client.rPush("LIST2", "1")
        client.rPush("LIST2", "2")
        client.rPush("LIST2", "3")

        client.rPush("LIST3", "0")
        client.rPush("LIST3", "1")
        client.rPush("LIST3", "2")
        client.rPush("LIST3", "3")

        client.lTrim("LIST", 0, 0)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("0")
        client.lTrim("LIST2", 0, -1)
        client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        client.lTrim("LIST2", -4, -1)
        client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List(
          "0", "1", "2", "3"
        )
        client.lTrim("LIST2", 0, -3)
        client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List("0", "1")
        client.lTrim("LIST3", 2, 3)
        client.lRange("LIST3").futureValue should contain theSameElementsInOrderAs List("2", "3")
        client.lTrim("LIST3", 1, 1)
        client.lRange("LIST3").futureValue should contain theSameElementsInOrderAs List("3")
        client.del("LIST", "LIST2", "LIST3")
      }
    }
  }
  
  RPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.rPop("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { client.rPop("HASH").! }
      }
    }
    "the list contains some elements" should {
      "pop the first element of the list" taggedAs (V100) in {
        client.rPush("LIST", "A")
        client.rPush("LIST", "B")
        client.rPop("LIST").futureValue should contain ("B")
        client.rPop("LIST").futureValue should contain ("A")
        client.rPop("LIST").futureValue should be (empty)
      }
    }
  }

  RPopLPush.toString when {
    "the source key does not exist" should {
      "return None and do nothing" taggedAs (V120) in {
        client.rPopLPush("LIST", "LIST2").futureValue should be (empty)
        client.lLen("LIST").futureValue should be (0)
        client.lLen("LIST2").futureValue should be (0)
      }
    }
    "the source and dest keys are identical" should {
      "do nothing" taggedAs (V120) in {
        client.rPush("LIST", "A")
        client.rPopLPush("LIST", "LIST").futureValue should contain ("A")
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A")
      }
    }
    "the dest key does not exist" should {
      "create a list at dest key and prepend the popped value" taggedAs (V120) in {
        client.rPopLPush("LIST", "LIST2").futureValue should contain ("A")
        client.lLen("LIST").futureValue should be (0)
        client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List("A")
      }
    }
    "one of the keys does not contain a list" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.rPopLPush("HASH", "LIST").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.rPopLPush("LIST2", "HASH").!
        }
      }
    }
    "both lists contain some elements" should {
      "pop the last element of list at source key and " +
        "prepend it to list at dest key" taggedAs (V120) in {
          client.rPush("LIST", "A")
          client.rPush("LIST", "B")
          client.rPush("LIST", "C")

          client.rPopLPush("LIST", "LIST2").futureValue should contain ("C")
          client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A", "B")
          client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List("C", "A")

          client.rPopLPush("LIST", "LIST2").futureValue should contain ("B")
          client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A")
          client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List(
            "B", "C", "A"
          )

          client.rPopLPush("LIST", "LIST2").futureValue should contain ("A")
          client.lRange("LIST").futureValue should be (empty)
          client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C", "A"
          )

          client.rPopLPush("LIST2", "LIST").futureValue should contain ("A")
          client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A")
          client.lRange("LIST2").futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C"
          )

          client.del("LIST", "LIST2")
        }
    }
  }
  
  RPush.toString when {
    "the key does not exist" should {
      "create a list and append the value" taggedAs (V100) in {
        client.rPush("LIST", "A").futureValue should be (1)
        client.rPush("LIST", "B").futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A", "B")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.rPush("HASH", "A").!
        }
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs (V100) in {
        client.rPush("LIST", "1").futureValue should be (3)
        client.rPush("LIST", "2").futureValue should be (4)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "1", "2"
        )
        client.del("LIST")
      }
    }
  }

  s"${RPush.toString}-2.4" when {
    "the key does not exist" should {
      "create a list and append the values" taggedAs (V240) in {
        client.rPush("LIST", "A", "B").futureValue should be (2)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List("A", "B")
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.rPush("HASH", "A", "B").!
        }
      }
    }
    "the list contains some elements" should {
      "append the values to the existing list" taggedAs (V240) in {
        client.rPush("LIST", "1", "2").futureValue should be (4)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "1", "2"
        )
        client.del("LIST")
      }
    }
  }
  
  RPushX.toString when {
    "the key does not exist" should {
      "do nothing" taggedAs (V220) in {
        client.rPushX("LIST", "A").futureValue should be (0)
        client.rPushX("LIST", "B").futureValue should be (0)
        client.lRange("LIST").futureValue should be (empty)
      }
    }
    "the key does not contain a list" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.rPushX("HASH", "A").!
        }
      }
    }
    "the list contains some elements" should {
      "append the value to the existing list" taggedAs (V220) in {
        client.rPush("LIST", "1")
        client.rPushX("LIST", "2").futureValue should be (2)
        client.rPushX("LIST", "3").futureValue should be (3)
        client.lRange("LIST").futureValue should contain theSameElementsInOrderAs List(
          "1", "2", "3"
        )
        client.del("LIST")
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
