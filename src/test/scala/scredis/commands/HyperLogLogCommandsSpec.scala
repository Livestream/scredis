package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.HyperLogLogRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

class HyperLogLogCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  
  override def beforeAll(): Unit = {
    client.lPush("LIST", "A").!
  }
  
  PFAdd.toString when {
    "the target key is not of type hyperloglog" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.pfAdd("LIST", "X").!
        }
      }
    }
    "the target key does not exist" should {
      "succeed" taggedAs (V289) in {
        client.pfAdd("HLL", "X").futureValue should be (true)
      }
    }
    "the target key exists" should {
      "succeed" taggedAs (V289) in {
        client.pfAdd("HLL", "Y").futureValue should be (true)
      }
    }
    "the element already exists" should {
      "succeed" taggedAs (V289) in {
        client.pfAdd("HLL", "X").futureValue should be (false)
      }
    }
  }
  
  PFCount.toString when {
    "one of the keys is not of type hyperloglog" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.pfCount("LIST").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.pfCount("LIST", "HLL").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.pfCount("HLL", "LIST").!
        }
      }
    }
    "the key does not exist" should {
      "return 0" taggedAs (V289) in {
        client.pfCount("HHH").futureValue should be (0)
      }
    }
    "the key exists" should {
      "return the count" taggedAs (V289) in {
        client.pfCount("HLL").futureValue should be >= (0l)
      }
    }
    "one of the key does not exist" should {
      "return the count of the ones that exist" taggedAs (V289) in {
        client.pfCount("HLL", "HHH").futureValue should be >= (0l)
      }
    }
    "multiple existent keys are provided" should {
      "return the merged count" taggedAs (V289) in {
        client.pfAdd("HLL2", "X")
        client.pfAdd("HLL2", "Z")
        client.pfCount("HLL", "HLL2").futureValue should be >= (0l)
      }
    }
  }
  
  PFMerge.toString when {
    "one of the keys is not of type hyperloglog" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.pfMerge("DEST", "LIST", "HLL").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.pfMerge("DEST", "LIST").!
        }
      }
    }
    "the key does not exist" should {
      "return 0" taggedAs (V289) in {
        client.pfMerge("DEST", "HHH").futureValue should be (())
        client.pfCount("DEST").futureValue should be (0)
      }
    }
    "the key exists" should {
      "merge and store the count" taggedAs (V289) in {
        client.pfMerge("DEST", "HLL").futureValue should be (())
        client.pfCount("DEST").futureValue should be >= (0l)
      }
    }
    "multiple existent keys are provided" should {
      "return the merged count" taggedAs (V289) in {
        client.pfMerge("DEST", "HLL", "HLL2").futureValue should be (())
        client.pfCount("DEST").futureValue should be >= (0l)
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }
  
}
