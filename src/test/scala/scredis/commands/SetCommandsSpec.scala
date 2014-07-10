package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.SetRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class SetCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    client.hSet("HASH", "FIELD", SomeValue).!
  }

  SAdd.toString when {
    "the key does not exist" should {
      "create a set and add the member to it" taggedAs (V100) in {
        client.sAdd("SET", SomeValue).futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(SomeValue)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sAdd("HASH", "hello").!
        }
      }
    }
    "the set contains some elements" should {
      "add the provided member only if it is not already contained in the set" taggedAs (V100) in {
        client.sAdd("SET", SomeValue).futureValue should be (0)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(SomeValue)
        client.sAdd("SET", "A").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(SomeValue, "A")
        client.del("SET")
      }
    }
  }

  s"${SAdd.toString}-2.4" when {
    "the key does not exist" should {
      "create a set and add the members to it" taggedAs (V240) in {
        client.sAdd("SET", SomeValue, "A").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(SomeValue, "A")
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sAdd("HASH", "hello", "asd").!
        }
      }
    }
    "the set contains some elements" should {
      "add the provided members only if it is not already contained in the set" taggedAs (V240) in {
        client.sAdd("SET", SomeValue, "A").futureValue should be (0)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(SomeValue, "A")
        client.sAdd("SET", "B", "C").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          SomeValue, "A", "B", "C"
        )
        client.del("SET")
      }
    }
  }

  SCard.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        client.sCard("SET").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sCard("HASH").!
        }
      }
    }
    "the set contains some elements" should {
      "return the number of element in the set" taggedAs (V100) in {
        client.sAdd("SET", "1")
        client.sAdd("SET", "2")
        client.sAdd("SET", "3")
        client.sCard("SET").futureValue should be (3)
        client.del("SET")
      }
    }
  }

  SDiff.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sDiff("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sDiff("SET1", "SET2", "SET3").futureValue should contain theSameElementsAs List("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sDiff("HASH", "SET1", "SET2").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.sDiff("SET1", "HASH", "SET2").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.sDiff("SET1", "SET2", "HASH").!
        }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sDiff("SET1", "SET1").futureValue should be (empty)
        client.sDiff("SET1", "SET2").futureValue should contain theSameElementsAs List(
          "A", "B", "D"
        )
        client.sDiff("SET1", "SET3").futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        client.sDiff("SET1", "SET2", "SET3").futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        client.del("SET1", "SET2", "SET3")
      }
    }
  }

  SDiffStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sDiffStore("SET", "SET1", "SET2", "SET3").futureValue should be (0)
        client.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sDiffStore("SET", "SET1", "SET2", "SET3").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.sDiffStore("SET", "HASH", "SET2", "SET3").!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.sDiffStore("SET", "SET1", "HASH", "SET3").!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.sDiffStore("SET", "SET1", "SET2", "HASH").!
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sDiffStore("SET", "SET1", "SET1").futureValue should be (0)
        client.sMembers("SET").futureValue should be (empty)

        client.sDiffStore("SET", "SET1", "SET2").futureValue should be (3)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "D"
        )

        client.sDiffStore("SET", "SET1", "SET3").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "B", "D"
        )

        client.sDiffStore("SET", "SET1", "SET2", "SET3").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "B", "D"
        )
        client.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SInter.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sInter("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sInter("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sInter("HASH", "SET1", "SET2").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.sInter("SET1", "HASH", "SET2").!
        }
        client.sAdd("SET2", "A")
        a [RedisErrorResponseException] should be thrownBy {
          client.sInter("SET1", "SET2", "HASH").!
        }
        client.del("SET2")
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sInter("SET1", "SET1").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        client.sInter("SET1", "SET2").futureValue should contain theSameElementsAs List(
          "C"
        )
        client.sInter("SET1", "SET3").futureValue should contain theSameElementsAs List(
          "A", "C"
        )
        client.sInter("SET1", "SET2", "SET3").futureValue should contain theSameElementsAs List(
          "C"
        )
        client.del("SET1", "SET2", "SET3")
      }
    }
  }

  SInterStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sInterStore("SET", "SET1", "SET2", "SET3").futureValue should be (0)
        client.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sInterStore("SET", "SET1", "SET2", "SET3").futureValue should be (0)
        client.sMembers("SET").futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.sInterStore("SET", "HASH", "SET2", "SET3").!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.sInterStore("SET", "SET1", "HASH", "SET3").!
        }
        client.sAdd("SET2", "A")
        a [RedisErrorResponseException] should be thrownBy { 
          client.sInterStore("SET", "SET1", "SET2", "HASH").!
        }
        client.del("SET2")
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sInterStore("SET", "SET1", "SET1").futureValue should be (4)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        client.sInterStore("SET", "SET1", "SET2").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "C"
        )

        client.sInterStore("SET", "SET1", "SET3").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "C"
        )

        client.sInterStore("SET", "SET1", "SET2", "SET3").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "C"
        )
        client.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SIsMember.toString when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.sIsMember("SET", "A").futureValue should be (false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sIsMember("HASH", "A").!
        }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        client.sAdd("SET", "1")
        client.sAdd("SET", "2")
        client.sAdd("SET", "3")
        client.sIsMember("SET", "A").futureValue should be (false)
        client.sIsMember("SET", "1").futureValue should be (true)
        client.sIsMember("SET", "2").futureValue should be (true)
        client.sIsMember("SET", "3").futureValue should be (true)
        client.del("SET")
      }
    }
  }

  SMembers.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.sMembers("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sMembers("HASH").!
        }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        client.sAdd("SET", "1")
        client.sAdd("SET", "2")
        client.sAdd("SET", "3")
        client.sMembers("SET").futureValue should contain theSameElementsAs List("1", "2", "3")
        client.del("SET")
      }
    }
  }

  SMove.toString when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.sMove("SET1", "SET2", "A").futureValue should be (false)
        client.sIsMember("SET2", "A").futureValue should be (false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sMove("HASH", "SET", "A").!
        }
        client.sAdd("SET", "A")
        a [RedisErrorResponseException] should be thrownBy {
          client.sMove("SET", "HASH", "A").!
        }
        client.del("SET")
      }
    }
    "the set contains some elements" should {
      "move the member from one set to another" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")

        client.sMove("SET1", "SET2", "B").futureValue should be (true)
        client.sMembers("SET1").futureValue should contain theSameElementsAs List("A", "C")
        client.sIsMember("SET2", "B").futureValue should be (true)
        client.del("SET1", "SET2")
      }
    }
  }

  SPop.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.sPop("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sPop("HASH").!
        }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs (V100) in {
        client.sAdd("SET", "A")
        client.sAdd("SET", "B")
        client.sAdd("SET", "C")

        val member1 = client.sPop("SET").!.get
        member1 shouldBe oneOf ("A", "B", "C")
        client.sIsMember("SET", member1).futureValue should be (false)

        val member2 = client.sPop("SET").!.get
        member2 shouldBe oneOf ("A", "B", "C")
        member2 should not be (member1)
        client.sIsMember("SET", member2).futureValue should be (false)

        val member3 = client.sPop("SET").!.get
        member3 shouldBe oneOf ("A", "B", "C")
        member3 should not be (member1)
        member3 should not be (member2)
        client.sIsMember("SET", member3).futureValue should be (false)

        client.sPop("SET").futureValue should be (empty)
      }
    }
  }

  SRandMember.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.sRandMember("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sRandMember("HASH").!
        }
      }
    }
    "the set contains some elements" should {
      "return a random member but do not remove it" taggedAs (V100) in {
        client.sAdd("SET", "A")
        client.sAdd("SET", "B")
        client.sAdd("SET", "C")

        client.sRandMember("SET").futureValue should contain oneOf ("A", "B", "C")
        client.sCard("SET").futureValue should be (3)
        client.del("SET")
      }
    }
  }
  
  s"${SRandMember.toString}-2.6" when {
    "the key does not exist" should {
      "return None" taggedAs (V260) in {
        client.sRandMembers("SET", 3).futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sRandMembers("HASH", 3).!
        }
      }
    }
    "the set contains some elements" should {
      "return count random members and do not remove them" taggedAs (V260) in {
        client.sAdd("SET", "A")
        client.sAdd("SET", "B")
        client.sAdd("SET", "C")
        client.sAdd("SET", "D")

        val members = client.sRandMembers("SET", 3).!
        members should have size (3)
        members should contain atMostOneOf ("A", "B", "C", "D")
        client.sCard("SET").futureValue should be (4)
        client.del("SET")
      }
    }
  }

  SRem.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        client.sRem("SET", "A").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sRem("HASH", "A").!
        }
      }
    }
    "the set contains some elements" should {
      "remove the member and return 1" taggedAs (V100) in {
        client.sAdd("SET", "A")
        client.sAdd("SET", "B")
        client.sAdd("SET", "C")

        client.sRem("SET", "B").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List("A", "C")

        client.sRem("SET", "B").futureValue should be (0)
        client.sMembers("SET").futureValue should contain theSameElementsAs List("A", "C")

        client.sRem("SET", "A").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List("C")

        client.sRem("SET", "C").futureValue should be (1)
        client.sMembers("SET").futureValue should be (empty)
      }
    }
  }

  s"${SRem.toString}-2.4" when {
    "the key does not exist" should {
      "return 0" taggedAs (V240) in {
        client.sRem("SET", "A", "B").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sRem("HASH", "A", "B").!
        }
      }
    }
    "the set contains some elements" should {
      "remove the members and return the number of members that were removed" taggedAs (V240) in {
        client.sAdd("SET", "A")
        client.sAdd("SET", "B")
        client.sAdd("SET", "C")

        client.sRem("SET", "B", "C").futureValue should be (2)
        client.sMembers("SET").futureValue should contain theSameElementsAs List("A")

        client.sRem("SET", "A", "B", "C").futureValue should be (1)
        client.sMembers("SET").futureValue should be (empty)

        client.sRem("SET", "A", "B", "C").futureValue should be (0)
      }
    }
  }
  
  SScan.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = client.sScan[String]("NONEXISTENTKEY", 0).!
        next should be (0)
        set should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sScan[String]("HASH", 0).!
        }
      }
    }
    "the set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.sAdd("SSET", "value" + i)
        }
        val (next, set) = client.sScan[String]("SSET", 0).!
        next should be (0)
        set should contain theSameElementsAs List("value1", "value2", "value3", "value4", "value5")
        for (i <- 1 to 10) {
          client.sAdd("SSET", "foo" + i)
        }
      }
    }
    "the set contains 15 elements" should {
      val full = ListBuffer[String]()
      for (i <- 1 to 5) {
        full += ("value" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullList = full.toList
      
      Given("that no pattern is set")
      "return all elements" taggedAs (V280) in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.sScan[String]("SSET", cursor).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.sScan[String]("SSET", cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs (V280) in {
        val elements = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.sScan[String](
            "SSET", cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size should be (10)
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
    }
  }
  
  SUnion.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sUnion("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sUnion("SET1", "SET2", "SET3").futureValue should contain theSameElementsAs List(
          "A"
        )
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.sUnion("HASH", "SET1", "SET2").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.sUnion("SET1", "HASH", "SET2").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.sUnion("SET1", "SET2", "HASH").!
        }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sUnion("SET1", "SET1").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        client.sUnion("SET1", "SET2").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )
        client.sUnion("SET1", "SET3").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        client.sUnion("SET1", "SET2", "SET3").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        client.del("SET1", "SET2", "SET3")
      }
    }
  }

  SUnionStore.toString when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        client.sUnionStore("SET", "SET1", "SET2", "SET3").futureValue should be (0)
        client.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        client.sAdd("SET1", "A")
        client.sUnionStore("SET", "SET1", "SET2", "SET3").futureValue should be (1)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A"
        )
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.sUnionStore("SET", "HASH", "SET2", "SET3").!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.sUnionStore("SET", "SET1", "HASH", "SET3").!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.sUnionStore("SET", "SET1", "SET2", "HASH").!
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        client.sAdd("SET1", "B")
        client.sAdd("SET1", "C")
        client.sAdd("SET1", "D")

        client.sAdd("SET2", "C")

        client.sAdd("SET3", "A")
        client.sAdd("SET3", "C")
        client.sAdd("SET3", "E")

        client.sUnionStore("SET", "SET1", "SET1").futureValue should be (4)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        client.sUnionStore("SET", "SET1", "SET2").futureValue should be (4)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D"
        )

        client.sUnionStore("SET", "SET1", "SET3").futureValue should be (5)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )

        client.sUnionStore("SET", "SET1", "SET2", "SET3").futureValue should be (5)
        client.sMembers("SET").futureValue should contain theSameElementsAs List(
          "A", "B", "C", "D", "E"
        )
        client.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }

}
