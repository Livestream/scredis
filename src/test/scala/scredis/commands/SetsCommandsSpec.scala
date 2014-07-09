package scredis.commands

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.Redis
import scredis.exceptions.RedisErrorResponseException
import scredis.tags._

import scala.collection.mutable.{ Set => MutableSet }

import java.util.concurrent.Executors

class SetsCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    redis.hSet("HASH")("FIELD", SomeValue)
  }

  import Names._
  import scredis.util.TestUtils._
  import redis.ec

  SAdd.name when {
    "the key does not exist" should {
      "create a set and add the member to it" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue).futureValue should be (1)
        redis.sMembers("SET").futureValue should contain(SomeValue)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sAdd("HASH", "hello").futureValue }
      }
    }
    "the set contains some elements" should {
      "add the provided member only if it is not already contained in the set" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue).futureValue should be (0)
        redis.sMembers("SET").futureValue should contain(SomeValue)
        redis.sAdd("SET", "A").futureValue should be (1)
        redis.sMembers("SET").futureValue should (contain(SomeValue) and contain("A"))
        redis.del("SET")
      }
    }
  }

  "%s-2.4".format(SAdd).name when {
    "the key does not exist" should {
      "create a set and add the members to it" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue, "A").futureValue should be (2)
        redis.sMembers("SET").futureValue should (contain(SomeValue) and contain("A"))
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sAdd("HASH", "hello", "asd").futureValue }
      }
    }
    "the set contains some elements" should {
      "add the provided members only if it is not already contained in the set" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue, "A").futureValue should be (0)
        redis.sMembers("SET").futureValue should (contain(SomeValue) and contain("A"))
        redis.sAdd("SET", "B", "C").futureValue should be (2)
        redis.sMembers("SET").futureValue should (
          contain(SomeValue) and contain("A") and contain("B") and contain("C")
        )
        redis.del("SET")
      }
    }
  }

  SCard.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sCard("SET").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sCard("HASH").futureValue }
      }
    }
    "the set contains some elements" should {
      "return the number of element in the set" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sCard("SET").futureValue should be (3)
        redis.del("SET")
      }
    }
  }

  SDiff.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sDiff("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sDiff("SET1", "SET2", "SET3").futureValue should contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sDiff("HASH", "SET1", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.sDiff("SET1", "HASH", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.sDiff("SET1", "SET2", "HASH").futureValue }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sDiff("SET1", "SET1").futureValue should be (empty)
        redis.sDiff("SET1", "SET2").futureValue should (contain("A") and contain("B") and contain("D"))
        redis.sDiff("SET1", "SET3").futureValue should (contain("B") and contain("D"))
        redis.sDiff("SET1", "SET2", "SET3").futureValue should (contain("B") and contain("D"))
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SDiffStore.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sDiffStore("SET")("SET1", "SET2", "SET3").futureValue should be (0)
        redis.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sDiffStore("SET")("SET1", "SET2", "SET3").futureValue should be (1)
        redis.sMembers("SET").futureValue should contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sDiffStore("SET")("HASH", "SET2", "SET3").futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sDiffStore("SET")("SET1", "HASH", "SET3").futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sDiffStore("SET")("SET1", "SET2", "HASH").futureValue
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sDiffStore("SET")("SET1", "SET1").futureValue should be (0)
        redis.sMembers("SET").futureValue should be (empty)

        redis.sDiffStore("SET")("SET1", "SET2").futureValue should be (3)
        redis.sMembers("SET").futureValue should (contain("A") and contain("B") and contain("D"))

        redis.sDiffStore("SET")("SET1", "SET3").futureValue should be (2)
        redis.sMembers("SET").futureValue should (contain("B") and contain("D"))

        redis.sDiffStore("SET")("SET1", "SET2", "SET3").futureValue should be (2)
        redis.sMembers("SET").futureValue should (contain("B") and contain("D"))
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SInter.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sInter("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sInter("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sInter("HASH", "SET1", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.sInter("SET1", "HASH", "SET2").futureValue }
        redis.sAdd("SET2", "A")
        a [RedisErrorResponseException] should be thrownBy { redis.sInter("SET1", "SET2", "HASH").futureValue }
        redis.del("SET2")
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sInter("SET1", "SET1").futureValue should (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sInter("SET1", "SET2").futureValue should contain("C")
        redis.sInter("SET1", "SET3").futureValue should (contain("A") and contain("C"))
        redis.sInter("SET1", "SET2", "SET3").futureValue should contain("C")
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SInterStore.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sInterStore("SET")("SET1", "SET2", "SET3").futureValue should be (0)
        redis.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sInterStore("SET")("SET1", "SET2", "SET3").futureValue should be (0)
        redis.sMembers("SET").futureValue should be (empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sInterStore("SET")("HASH", "SET2", "SET3").futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sInterStore("SET")("SET1", "HASH", "SET3").futureValue
        }
        redis.sAdd("SET2", "A")
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sInterStore("SET")("SET1", "SET2", "HASH").futureValue
        }
        redis.del("SET2")
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sInterStore("SET")("SET1", "SET1").futureValue should be (4)
        redis.sMembers("SET").futureValue should (contain("A") and contain("B") and contain("C") and
          contain("D"))

        redis.sInterStore("SET")("SET1", "SET2").futureValue should be (1)
        redis.sMembers("SET").futureValue should contain("C")

        redis.sInterStore("SET")("SET1", "SET3").futureValue should be (2)
        redis.sMembers("SET").futureValue should (contain("A") and contain("C"))

        redis.sInterStore("SET")("SET1", "SET2", "SET3").futureValue should be (1)
        redis.sMembers("SET").futureValue should contain("C")
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SUnion.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sUnion("SET1", "SET2", "SET3").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sUnion("SET1", "SET2", "SET3").futureValue should contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sUnion("HASH", "SET1", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.sUnion("SET1", "HASH", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.sUnion("SET1", "SET2", "HASH").futureValue }
      }
    }
    "the sets contain some elements" should {
      "return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sUnion("SET1", "SET1").futureValue should (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sUnion("SET1", "SET2").futureValue should (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sUnion("SET1", "SET3").futureValue should (contain("A") and contain("B") and contain("C") and
          contain("D") and contain("E"))
        redis.sUnion("SET1", "SET2", "SET3").futureValue should (contain("A") and contain("B") and
          contain("C") and contain("D") and contain("E"))
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SUnionStore.name when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sUnionStore("SET")("SET1", "SET2", "SET3").futureValue should be (0)
        redis.sCard("SET").futureValue should be (0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sUnionStore("SET")("SET1", "SET2", "SET3").futureValue should be (1)
        redis.sMembers("SET").futureValue should contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sUnionStore("SET")("HASH", "SET2", "SET3").futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sUnionStore("SET")("SET1", "HASH", "SET3").futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.sUnionStore("SET")("SET1", "SET2", "HASH").futureValue
        }
      }
    }
    "the sets contain some elements" should {
      "store resulting set at destKey" taggedAs (V100) in {
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")
        redis.sAdd("SET1", "D")

        redis.sAdd("SET2", "C")

        redis.sAdd("SET3", "A")
        redis.sAdd("SET3", "C")
        redis.sAdd("SET3", "E")

        redis.sUnionStore("SET")("SET1", "SET1").futureValue should be (4)
        redis.sMembers("SET").futureValue should (
          contain("A") and contain("B") and contain("C") and contain("D")
        )

        redis.sUnionStore("SET")("SET1", "SET2").futureValue should be (4)
        redis.sMembers("SET").futureValue should (
          contain("A") and contain("B") and contain("C") and contain("D")
        )

        redis.sUnionStore("SET")("SET1", "SET3").futureValue should be (5)
        redis.sMembers("SET").futureValue should (
          contain("A") and contain("B") and contain("C") and contain("D") and contain("E")
        )

        redis.sUnionStore("SET")("SET1", "SET2", "SET3").futureValue should be (5)
        redis.sMembers("SET").futureValue should (
          contain("A") and contain("B") and contain("C") and contain("D") and contain("E")
        )
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SIsMember.name when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        redis.sIsMember("SET", "A").futureValue should be (false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sIsMember("HASH", "A").futureValue }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sIsMember("SET", "A").futureValue should be (false)
        redis.sIsMember("SET", "1").futureValue should be (true)
        redis.sIsMember("SET", "2").futureValue should be (true)
        redis.sIsMember("SET", "3").futureValue should be (true)
        redis.del("SET")
      }
    }
  }

  SMembers.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sMembers("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sMembers("HASH").futureValue }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sMembers("SET").futureValue should (contain("1") and contain("2") and contain("3"))
        redis.del("SET")
      }
    }
  }

  SMove.name when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        redis.sMove("SET1", "A")("SET2").futureValue should be (false)
        redis.sIsMember("SET2", "A").futureValue should be (false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sMove("HASH", "A")("SET").futureValue }
        redis.sAdd("SET", "A")
        a [RedisErrorResponseException] should be thrownBy { redis.sMove("SET", "A")("HASH").futureValue }
        redis.del("SET")
      }
    }
    "the set contains some elements" should {
      "move the member from one set to another" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")

        redis.sMove("SET1", "B")("SET2").futureValue should be (true)
        redis.sMembers("SET1").futureValue should (contain("A") and contain("C") and not contain ("B"))
        redis.sIsMember("SET2", "B").futureValue should be (true)
        redis.del("SET1", "SET2")
      }
    }
  }

  SPop.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sPop("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sPop("HASH").futureValue }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        val member1 = redis.sPop("SET").futureValue.get
        member1 should (be ("A") or be ("B") or be ("C"))
        redis.sIsMember("SET", member1).futureValue should be (false)

        val member2 = redis.sPop("SET").futureValue.get
        member2 should ((not be (member1)) and (be ("A") or be ("B") or be ("C")))
        redis.sIsMember("SET", member2).futureValue should be (false)

        val member3 = redis.sPop("SET").futureValue.get
        member3 should ((not be (member1) and not be (member2)) and (be ("A") or be ("B") or be ("C")))
        redis.sIsMember("SET", member3).futureValue should be (false)

        redis.sPop("SET").futureValue should be (empty)
      }
    }
  }

  SRandMember.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sRandMember("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sRandMember("HASH").futureValue }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        val member = redis.sRandMember("SET").futureValue.get
        member should (be ("A") or be ("B") or be ("C"))
        redis.sIsMember("SET", member).futureValue should be (true)
        redis.sCard("SET").futureValue should be (3)

        redis.del("SET")
      }
    }
  }

  SRem.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sRem("SET", "A").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sRem("HASH", "A").futureValue }
      }
    }
    "the set contains some elements" should {
      "remove the member and return 1" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        redis.sRem("SET", "B").futureValue should be (1)
        redis.sMembers("SET").futureValue should (contain("A") and contain("C") and not contain ("B"))

        redis.sRem("SET", "B").futureValue should be (0)
        redis.sMembers("SET").futureValue should (contain("A") and contain("C") and not contain ("B"))

        redis.sRem("SET", "A").futureValue should be (1)
        redis.sMembers("SET").futureValue should (contain("C") and not contain ("A") and not contain ("B"))

        redis.sRem("SET", "C").futureValue should be (1)
        redis.sMembers("SET").futureValue should be (empty)
      }
    }
  }

  "%s-2.4".format(SRem).name when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sRem("SET", "A", "B").futureValue should be (0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sRem("HASH", "A", "B").futureValue }
      }
    }
    "the set contains some elements" should {
      "remove the members and return the number of members that were removed" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        redis.sRem("SET", "B", "C").futureValue should be (2)
        redis.sMembers("SET").futureValue should (contain("A") and not contain ("B") and not contain ("C"))

        redis.sRem("SET", "A", "B", "C").futureValue should be (1)
        redis.sMembers("SET").futureValue should be (empty)

        redis.sRem("SET", "A", "B", "C").futureValue should be (0)
      }
    }
  }
  
  SScan.name when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = redis.sScan[String]("NONEXISTENTKEY")(0).futureValue
        next should be (0)
        set should be (empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy { redis.sScan[String]("HASH")(0).futureValue }
      }
    }
    "the set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          redis.sAdd("SSET", "value" + i).futureValue
        }
        val (next, set) = redis.sScan[String]("SSET")(0).futureValue
        next should be (0)
        set should (
          contain("value1") and
          contain("value2") and
          contain("value3") and
          contain("value4") and
          contain("value5")
        )
        for (i <- 1 to 10) {
          redis.sAdd("SSET", "foo" + i).futureValue
        }
      }
    }
    "the set contains 15 elements" should {
      val full = MutableSet[String]()
      for (i <- 1 to 5) {
        full += ("value" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullSet = full.toSet
      
      Given("that no pattern is set")
      "return all elements" taggedAs (V280) in {
        val elements = MutableSet[String]()
        var cursor = 0L
        do {
          val (next, set) = redis.sScan[String]("SSET")(cursor).futureValue
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet should be (fullSet)
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = MutableSet[String]()
        var cursor = 0L
        do {
          val (next, set) = redis.sScan[String]("SSET")(cursor, matchOpt = Some("foo*")).futureValue
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet should be (fullSet.filter(_.startsWith("foo")))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs (V280) in {
        val elements = MutableSet[String]()
        var cursor = 0L
        do {
          val (next, set) = redis.sScan[String]("SSET")(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).futureValue
          set.size should be (10)
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet should be (fullSet.filter(_.startsWith("foo")))
      }
    }
  }

  override def afterAll() {
    redis.flushDb().futureValue
    redis.quit()
  }

}
