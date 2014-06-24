package scredis.commands

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.Redis
import scredis.exceptions.RedisCommandException
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

  SAdd when {
    "the key does not exist" should {
      "create a set and add the member to it" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue) must be(1)
        redis.sMembers("SET") must contain(SomeValue)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sAdd("HASH", "hello").! }
      }
    }
    "the set contains some elements" should {
      "add the provided member only if it is not already contained in the set" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue) must be(0)
        redis.sMembers("SET") must contain(SomeValue)
        redis.sAdd("SET", "A") must be(1)
        redis.sMembers("SET") must (contain(SomeValue) and contain("A"))
        redis.del("SET")
      }
    }
  }

  "%s-2.4".format(SAdd) when {
    "the key does not exist" should {
      "create a set and add the members to it" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue, "A") must be(2)
        redis.sMembers("SET") must (contain(SomeValue) and contain("A"))
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sAdd("HASH", "hello", "asd").! }
      }
    }
    "the set contains some elements" should {
      "add the provided members only if it is not already contained in the set" taggedAs (V100) in {
        redis.sAdd("SET", SomeValue, "A") must be(0)
        redis.sMembers("SET") must (contain(SomeValue) and contain("A"))
        redis.sAdd("SET", "B", "C") must be(2)
        redis.sMembers("SET") must (
          contain(SomeValue) and contain("A") and contain("B") and contain("C")
        )
        redis.del("SET")
      }
    }
  }

  SCard when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sCard("SET") must be(0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sCard("HASH").! }
      }
    }
    "the set contains some elements" should {
      "return the number of element in the set" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sCard("SET") must be(3)
        redis.del("SET")
      }
    }
  }

  SDiff when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sDiff("SET1", "SET2", "SET3") must be('empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sDiff("SET1", "SET2", "SET3") must contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sDiff("HASH", "SET1", "SET2").! }
        an [RedisCommandException] must be thrownBy { redis.sDiff("SET1", "HASH", "SET2").! }
        an [RedisCommandException] must be thrownBy { redis.sDiff("SET1", "SET2", "HASH").! }
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

        redis.sDiff("SET1", "SET1") must be('empty)
        redis.sDiff("SET1", "SET2") must (contain("A") and contain("B") and contain("D"))
        redis.sDiff("SET1", "SET3") must (contain("B") and contain("D"))
        redis.sDiff("SET1", "SET2", "SET3") must (contain("B") and contain("D"))
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SDiffStore when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sDiffStore("SET")("SET1", "SET2", "SET3") must be(0)
        redis.sCard("SET") must be(0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sDiffStore("SET")("SET1", "SET2", "SET3") must be(1)
        redis.sMembers("SET") must contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { 
          redis.sDiffStore("SET")("HASH", "SET2", "SET3").!
        }
        an [RedisCommandException] must be thrownBy { 
          redis.sDiffStore("SET")("SET1", "HASH", "SET3").!
        }
        an [RedisCommandException] must be thrownBy { 
          redis.sDiffStore("SET")("SET1", "SET2", "HASH").!
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

        redis.sDiffStore("SET")("SET1", "SET1") must be(0)
        redis.sMembers("SET") must be('empty)

        redis.sDiffStore("SET")("SET1", "SET2") must be(3)
        redis.sMembers("SET") must (contain("A") and contain("B") and contain("D"))

        redis.sDiffStore("SET")("SET1", "SET3") must be(2)
        redis.sMembers("SET") must (contain("B") and contain("D"))

        redis.sDiffStore("SET")("SET1", "SET2", "SET3") must be(2)
        redis.sMembers("SET") must (contain("B") and contain("D"))
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SInter when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sInter("SET1", "SET2", "SET3") must be('empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sInter("SET1", "SET2", "SET3") must be('empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sInter("HASH", "SET1", "SET2").! }
        an [RedisCommandException] must be thrownBy { redis.sInter("SET1", "HASH", "SET2").! }
        redis.sAdd("SET2", "A")
        an [RedisCommandException] must be thrownBy { redis.sInter("SET1", "SET2", "HASH").! }
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

        redis.sInter("SET1", "SET1") must (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sInter("SET1", "SET2") must contain("C")
        redis.sInter("SET1", "SET3") must (contain("A") and contain("C"))
        redis.sInter("SET1", "SET2", "SET3") must contain("C")
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SInterStore when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sInterStore("SET")("SET1", "SET2", "SET3") must be(0)
        redis.sCard("SET") must be(0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sInterStore("SET")("SET1", "SET2", "SET3") must be(0)
        redis.sMembers("SET") must be('empty)
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { 
          redis.sInterStore("SET")("HASH", "SET2", "SET3").!
        }
        an [RedisCommandException] must be thrownBy { 
          redis.sInterStore("SET")("SET1", "HASH", "SET3").!
        }
        redis.sAdd("SET2", "A")
        an [RedisCommandException] must be thrownBy { 
          redis.sInterStore("SET")("SET1", "SET2", "HASH").!
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

        redis.sInterStore("SET")("SET1", "SET1") must be(4)
        redis.sMembers("SET") must (contain("A") and contain("B") and contain("C") and
          contain("D"))

        redis.sInterStore("SET")("SET1", "SET2") must be(1)
        redis.sMembers("SET") must contain("C")

        redis.sInterStore("SET")("SET1", "SET3") must be(2)
        redis.sMembers("SET") must (contain("A") and contain("C"))

        redis.sInterStore("SET")("SET1", "SET2", "SET3") must be(1)
        redis.sMembers("SET") must contain("C")
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SUnion when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sUnion("SET1", "SET2", "SET3") must be('empty)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sUnion("SET1", "SET2", "SET3") must contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sUnion("HASH", "SET1", "SET2").! }
        an [RedisCommandException] must be thrownBy { redis.sUnion("SET1", "HASH", "SET2").! }
        an [RedisCommandException] must be thrownBy { redis.sUnion("SET1", "SET2", "HASH").! }
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

        redis.sUnion("SET1", "SET1") must (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sUnion("SET1", "SET2") must (contain("A") and contain("B") and contain("C") and
          contain("D"))
        redis.sUnion("SET1", "SET3") must (contain("A") and contain("B") and contain("C") and
          contain("D") and contain("E"))
        redis.sUnion("SET1", "SET2", "SET3") must (contain("A") and contain("B") and
          contain("C") and contain("D") and contain("E"))
        redis.del("SET1", "SET2", "SET3")
      }
    }
  }

  SUnionStore when {
    "all keys do not exist" should {
      "return None" taggedAs (V100) in {
        redis.sUnionStore("SET")("SET1", "SET2", "SET3") must be(0)
        redis.sCard("SET") must be(0)
      }
    }
    "some keys do not exist" should {
      "assume empty sets and return the resulting set" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sUnionStore("SET")("SET1", "SET2", "SET3") must be(1)
        redis.sMembers("SET") must contain("A")
      }
    }
    "at least one key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { 
          redis.sUnionStore("SET")("HASH", "SET2", "SET3").!
        }
        an [RedisCommandException] must be thrownBy { 
          redis.sUnionStore("SET")("SET1", "HASH", "SET3").!
        }
        an [RedisCommandException] must be thrownBy { 
          redis.sUnionStore("SET")("SET1", "SET2", "HASH").!
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

        redis.sUnionStore("SET")("SET1", "SET1") must be(4)
        redis.sMembers("SET") must (
          contain("A") and contain("B") and contain("C") and contain("D")
        )

        redis.sUnionStore("SET")("SET1", "SET2") must be(4)
        redis.sMembers("SET") must (
          contain("A") and contain("B") and contain("C") and contain("D")
        )

        redis.sUnionStore("SET")("SET1", "SET3") must be(5)
        redis.sMembers("SET") must (
          contain("A") and contain("B") and contain("C") and contain("D") and contain("E")
        )

        redis.sUnionStore("SET")("SET1", "SET2", "SET3") must be(5)
        redis.sMembers("SET") must (
          contain("A") and contain("B") and contain("C") and contain("D") and contain("E")
        )
        redis.del("SET", "SET1", "SET2", "SET3")
      }
    }
  }

  SIsMember when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        redis.sIsMember("SET", "A") must be(false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sIsMember("HASH", "A").! }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sIsMember("SET", "A") must be(false)
        redis.sIsMember("SET", "1") must be(true)
        redis.sIsMember("SET", "2") must be(true)
        redis.sIsMember("SET", "3") must be(true)
        redis.del("SET")
      }
    }
  }

  SMembers when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sMembers("SET") must be('empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sMembers("HASH").! }
      }
    }
    "the set contains some elements" should {
      "return the correct value" taggedAs (V100) in {
        redis.sAdd("SET", "1")
        redis.sAdd("SET", "2")
        redis.sAdd("SET", "3")
        redis.sMembers("SET") must (contain("1") and contain("2") and contain("3"))
        redis.del("SET")
      }
    }
  }

  SMove when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        redis.sMove("SET1", "A")("SET2") must be(false)
        redis.sIsMember("SET2", "A") must be(false)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sMove("HASH", "A")("SET").! }
        redis.sAdd("SET", "A")
        an [RedisCommandException] must be thrownBy { redis.sMove("SET", "A")("HASH").! }
        redis.del("SET")
      }
    }
    "the set contains some elements" should {
      "move the member from one set to another" taggedAs (V100) in {
        redis.sAdd("SET1", "A")
        redis.sAdd("SET1", "B")
        redis.sAdd("SET1", "C")

        redis.sMove("SET1", "B")("SET2") must be(true)
        redis.sMembers("SET1") must (contain("A") and contain("C") and not contain ("B"))
        redis.sIsMember("SET2", "B") must be(true)
        redis.del("SET1", "SET2")
      }
    }
  }

  SPop when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sPop("SET") must be('empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sPop("HASH").! }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        val member1 = redis.sPop("SET").!.get
        member1 must (be("A") or be("B") or be("C"))
        redis.sIsMember("SET", member1) must be(false)

        val member2 = redis.sPop("SET").!.get
        member2 must ((not be (member1)) and (be("A") or be("B") or be("C")))
        redis.sIsMember("SET", member2) must be(false)

        val member3 = redis.sPop("SET").!.get
        member3 must ((not be (member1) and not be (member2)) and (be("A") or be("B") or be("C")))
        redis.sIsMember("SET", member3) must be(false)

        redis.sPop("SET") must be('empty)
      }
    }
  }

  SRandMember when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        redis.sRandMember("SET") must be('empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sRandMember("HASH").! }
      }
    }
    "the set contains some elements" should {
      "return a random member and remove it" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        val member = redis.sRandMember("SET").!.get
        member must (be("A") or be("B") or be("C"))
        redis.sIsMember("SET", member) must be(true)
        redis.sCard("SET") must be(3)

        redis.del("SET")
      }
    }
  }

  SRem when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sRem("SET", "A") must be(0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sRem("HASH", "A").! }
      }
    }
    "the set contains some elements" should {
      "remove the member and return 1" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        redis.sRem("SET", "B") must be(1)
        redis.sMembers("SET") must (contain("A") and contain("C") and not contain ("B"))

        redis.sRem("SET", "B") must be(0)
        redis.sMembers("SET") must (contain("A") and contain("C") and not contain ("B"))

        redis.sRem("SET", "A") must be(1)
        redis.sMembers("SET") must (contain("C") and not contain ("A") and not contain ("B"))

        redis.sRem("SET", "C") must be(1)
        redis.sMembers("SET") must be('empty)
      }
    }
  }

  "%s-2.4".format(SRem) when {
    "the key does not exist" should {
      "return 0" taggedAs (V100) in {
        redis.sRem("SET", "A", "B") must be(0)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.sRem("HASH", "A", "B").! }
      }
    }
    "the set contains some elements" should {
      "remove the members and return the number of members that were removed" taggedAs (V100) in {
        redis.sAdd("SET", "A")
        redis.sAdd("SET", "B")
        redis.sAdd("SET", "C")

        redis.sRem("SET", "B", "C") must be(2)
        redis.sMembers("SET") must (contain("A") and not contain ("B") and not contain ("C"))

        redis.sRem("SET", "A", "B", "C") must be(1)
        redis.sMembers("SET") must be('empty)

        redis.sRem("SET", "A", "B", "C") must be(0)
      }
    }
  }
  
  SScan when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = redis.sScan[String]("NONEXISTENTKEY")(0).!
        next must be(0)
        set must be ('empty)
      }
    }
    "the key does not contain a set" should {
      "return an error" taggedAs (V280) in {
        an [RedisCommandException] must be thrownBy { redis.sScan[String]("HASH")(0).! }
      }
    }
    "the set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          redis.sAdd("SSET", "value" + i).!
        }
        val (next, set) = redis.sScan[String]("SSET")(0).!
        next must be(0)
        set must (
          contain("value1") and
          contain("value2") and
          contain("value3") and
          contain("value4") and
          contain("value5")
        )
        for (i <- 1 to 10) {
          redis.sAdd("SSET", "foo" + i).!
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
          val (next, set) = redis.sScan[String]("SSET")(cursor).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet)
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = MutableSet[String]()
        var cursor = 0L
        do {
          val (next, set) = redis.sScan[String]("SSET")(cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet.filter(_.startsWith("foo")))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs (V280) in {
        val elements = MutableSet[String]()
        var cursor = 0L
        do {
          val (next, set) = redis.sScan[String]("SSET")(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size must be(10)
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.toSet must be(fullSet.filter(_.startsWith("foo")))
      }
    }
  }

  override def afterAll() {
    redis.flushDb().!
    redis.quit()
  }

}
