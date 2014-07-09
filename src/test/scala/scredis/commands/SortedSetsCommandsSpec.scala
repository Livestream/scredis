package scredis.commands

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.Redis
import scredis.Score._
import scredis.Aggregate._
import scredis.exceptions.RedisErrorResponseException
import scredis.util.LinkedHashSet
import scredis.tags._

import math.{ min, max }

import java.util.concurrent.Executors

class SortedSetsCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    redis.hSet("HASH")("FIELD", SomeValue)
  }

  import Names._
  import scredis.util.TestUtils._
  import redis.ec

  ZAdd.name when {
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V120) in {
        redis.zAdd("SET", (SomeValue, 1)).futureValue should be (1)
        redis.zRange("SET").futureValue should contain(SomeValue)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zAdd("HASH", ("hello", 1)).futureValue }
      }
    }
    "the sorted set contains some elements" should {
      "add the provided member only if it is not already contained in the " +
        "sorted set" taggedAs (V120) in {
          redis.zAdd("SET", (SomeValue, 0)).futureValue should be (0)
          redis.zAdd("SET", ("A", -1.3))
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", -1.3), (SomeValue, 0)))
          redis.del("SET")
        }
    }
  }

  "%s-2.4".format(ZAdd).name when {
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1), ("B", 1.5)).futureValue should be (2)
        redis.zRange("SET").futureValue should (contain("A") and contain("B"))
        redis.del("SET")
        redis.zAddFromMap("SET", Map("A" -> 1, "B" -> 1.5)).futureValue should be (2)
      }
    }
    "providing an empty map" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zAddFromMap("SET", Map()).futureValue }
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zAddFromMap("HASH", Map("hello" -> 1, "asd" -> 2)).futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zAdd("HASH", ("hello", 1), ("asd", 2)).futureValue
        }
      }
    }
    "the sorted set contains some elements" should {
      "add the provided members only if they are not already contained " +
        "in the sorted set" taggedAs (V120) in {
          redis.zAdd("SET", ("A", 2.5), ("B", 3.8)).futureValue should be (0)
          redis.zAddFromMap("SET", Map("C" -> -1.3, "D" -> -2.6)).futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("D", -2.6), ("C", -1.3), ("A", 2.5), ("B", 3.8)
          ))
          redis.del("SET")
        }
    }
  }

  ZCard.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zCard("SET").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zCard("HASH").futureValue }
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET", ("B", 0))
        redis.zAdd("SET", ("C", 0))
        redis.zCard("SET").futureValue should be (3)
        redis.del("SET")
      }
    }
  }

  ZCount.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        redis.zCount("SET", Infinity, Infinity).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zCount("HASH", Infinity, Infinity).futureValue
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V200) in {
        redis.zAdd("SET", ("A", -1.5))
        redis.zAdd("SET", ("B", 0.4))
        redis.zAdd("SET", ("C", 1.6))
        redis.zAdd("SET", ("D", 3))
        redis.zCount("SET", inclusive(-3), inclusive(-3)).futureValue should be (0)
        redis.zCount("SET", inclusive(4), inclusive(4)).futureValue should be (0)
        redis.zCount("SET", inclusive(-1.5), inclusive(3)).futureValue should be (4)
        redis.zCount("SET", exclusive(-1.5), exclusive(3)).futureValue should be (2)
        redis.zCount("SET", inclusive(-1.5), exclusive(3)).futureValue should be (3)
        redis.zCount("SET", exclusive(-1.5), inclusive(3)).futureValue should be (3)
        redis.zCount("SET", exclusive(0), exclusive(0.5)).futureValue should be (1)
        redis.zCount("SET", inclusive(0.5), Infinity).futureValue should be (2)
        redis.zCount("SET", Infinity, inclusive(0.5)).futureValue should be (2)
        redis.zCount("SET", Infinity, Infinity).futureValue should be (4)
        redis.del("SET")
      }
    }
  }

  ZIncrBy.name when {
    "the key does not exist" should {
      "create a sorted set, add the member and increment the score starting " +
        "from zero" taggedAs (V120) in {
          redis.zIncrBy("SET", "A", 1.5).futureValue should be (1.5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 1.5)))
        }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zIncrBy("HASH", "A", 1.5).futureValue }
      }
    }
    "the sorted set contains some elements" should {
      "increment the score of the given member" taggedAs (V120) in {
        redis.zIncrBy("SET", "A", 1.5).futureValue should be (3.0)
        redis.zIncrBy("SET", "A", -0.5).futureValue should be (2.5)
        redis.zIncrBy("SET", "B", -0.7).futureValue should be (-0.7)
        redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("B", -0.7), ("A", 2.5)))
        redis.del("SET")
      }
    }
  }

  ZInterStore.name when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        redis.zInterStore("SET")("SET1", "SET2").futureValue should be (0)
        redis.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET1", ("A", 0))
        redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 0)))
        redis.zInterStore("SET")("SET1", "SET2", "SET3").futureValue should be (0)
        redis.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zInterStore("SET")("HASH").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.zInterStore("SET")("HASH", "SET2").futureValue }
      }
    }
    "the sorted sets contain some elements" should {
      Given("that the aggregation function is Sum")
      "compute the intersection between them, aggregate the scores with Sum and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zAdd("SET1", ("B", 1.7))
          redis.zAdd("SET1", ("C", 2.3))
          redis.zAdd("SET1", ("D", 4.41))

          redis.zAdd("SET2", ("C", 5.5))

          redis.zAdd("SET3", ("A", -1.0))
          redis.zAdd("SET3", ("C", -2.13))
          redis.zAdd("SET3", ("E", -5.56))

          redis.zInterStore("SET")("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zInterStore("SET")("SET1", "SET2").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", 2.3 + 5.5)))

          redis.zInterStore("SET")("SET1", "SET3").futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0 + (-1.0)), ("C", 2.3 + (-2.13))))

          redis.zInterStore("SET")("SET1", "SET2", "SET3").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", 2.3 + 5.5 + (-2.13))))
        }
      Given("that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zInterStore("SET", Min)("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)))

          redis.zInterStore("SET", Min)("SET1", "SET2").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", min(2.3, 5.5))))

          redis.zInterStore("SET", Min)("SET1", "SET3").futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", -2.13), ("A", -1)))

          redis.zInterStore("SET", Min)("SET1", "SET2", "SET3").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", min(min(2.3, 5.5), -2.13))))
        }
      Given("that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zInterStore("SET", Max)("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zInterStore("SET", Max)("SET1", "SET2").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", max(2.3, 5.5))))

          redis.zInterStore("SET", Max)("SET1", "SET3").futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 0), ("C", 2.3)))

          redis.zInterStore("SET", Max)("SET1", "SET2", "SET3").futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", max(max(2.3, 5.5), -2.13))))
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the intersection between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 3 * 0), ("B", 3 * 1.7), ("C", 3 * 2.3), ("D", 3 * 4.41)
          ))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET2", 2)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", 2.3 + 2 * 5.5)))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET3", 2)).futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0 + 2 * (-1.0)), ("C", 2.3 + 2 * (-2.13))
          ))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("C", 2.3 + 2 * 5.5 + (-1) * (-2.13))
          ))
        }
      Given("some custom weights and that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", min(2.3, 2 * 5.5))))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET3", 2)).futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", -4.26), ("A", -2)))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("C", min(min(2.3, 2 * 5.5), (-1) * (-2.13)))
          ))
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("C", max(2.3, 2 * 5.5))))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET3", 2)).futureValue should be (2)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 0), ("C", 2.3)))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (1)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("C", max(max(2.3, 2 * 5.5), (-1) * (-2.13)))
          ))

          redis.del("SET1", "SET2", "SET3")
        }
    }
  }

  ZUnionStore.name when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        redis.zUnionStore("SET")("SET1", "SET2").futureValue should be (0)
        redis.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET1", ("A", 0))
        redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 0)))
        redis.zUnionStore("SET")("SET1", "SET2", "SET3").futureValue should be (1)
        redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(("A", 0)))
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zUnionStore("SET")("HASH").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.zUnionStore("SET")("HASH", "SET2").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.zUnionStore("SET")("SET1", "HASH").futureValue }
      }
    }
    "the sorted sets contain some elements" should {
      Given("that the aggregation function is Sum")
      "compute the union between them, aggregate the scores with Sum and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zAdd("SET1", ("B", 1.7))
          redis.zAdd("SET1", ("C", 2.3))
          redis.zAdd("SET1", ("D", 4.41))

          redis.zAdd("SET2", ("C", 5.5))

          redis.zAdd("SET3", ("A", -1.0))
          redis.zAdd("SET3", ("C", -2.13))
          redis.zAdd("SET3", ("E", -5.56))

          redis.zUnionStore("SET")("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zUnionStore("SET")("SET1", "SET2").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5)
          ))

          redis.zUnionStore("SET")("SET1", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("A", -1), ("C", 2.3 - 2.13), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStore("SET")("SET1", "SET2", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("A", -1), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5 - 2.13)
          ))
        }
      Given("that the aggregation function is Min")
      "compute the union between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zUnionStore("SET", Min)("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET2").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET2", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          ))
        }
      Given("that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zUnionStore("SET", Max)("SET1", "SET1").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET2").futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET2", "SET3").futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", -5.56), ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          ))
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the union between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 3 * 0), ("B", 3 * 1.7), ("C", 3 * 2.3), ("D", 3 * 4.41)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET2", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 2 * 5.5)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET3", 2)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", 2 * (-5.56)), ("A", 2 * (-1)), ("C", 2.3 + 2 * (-2.13)), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 1),
            ("B", 1.7),
            ("D", 4.41),
            ("E", (-1) * (-5.56)),
            ("C", 2.3 + 2 * 5.5 + (-1) * (-2.13))
          ))
        }
      Given("some custom weights and that the aggregation function is Min")
      "compute the union between them, aggregate the scores with Min by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET3", 2)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", 2 * (-5.56)), ("C", -4.26), ("A", -2), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.13), ("D", 4.41), ("E", 5.56)
          ))
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET1", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 3.4), ("C", 4.6), ("D", 8.82)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2)).futureValue should be (4)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 11)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET3", 2)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("E", 2 * (-5.56)), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2), ("SET3", -1)).futureValue should be (5)
          redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
            ("A", 1), ("B", 1.7), ("D", 4.41), ("E", 5.56), ("C", 11)
          ))
          redis.del("SET", "SET1", "SET2", "SET3")
        }
    }
  }

  ZRange.name when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zRange("SET").futureValue should be (empty)
        redis.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRange("HASH").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.zRangeWithScores("HASH").futureValue }
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        redis.zAdd("SET", ("A", -5))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("D", 3))

        redis.zRange("SET", 0, 0).futureValue should be (LinkedHashSet("A"))
        redis.zRangeWithScores("SET", 0, 0).futureValue should be (LinkedHashSet(("A", -5)))

        redis.zRange("SET", 3, 3).futureValue should be (LinkedHashSet("D"))
        redis.zRangeWithScores("SET", 3, 3).futureValue should be (LinkedHashSet(("D", 3)))

        redis.zRange("SET", 1, 2).futureValue should be (LinkedHashSet("B", "C"))
        redis.zRangeWithScores("SET", 1, 2).futureValue should be (LinkedHashSet(("B", -1), ("C", 0)))

        redis.zRange("SET", 0, 3).futureValue should be (LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET", 0, 3).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET", 0, -1).futureValue should be (LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET", 0, -1).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET", 0, -2).futureValue should be (LinkedHashSet("A", "B", "C"))
        redis.zRangeWithScores("SET", 0, -2).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0)
        ))

        redis.zRange("SET", -3, -1).futureValue should be (LinkedHashSet("B", "C", "D"))
        redis.zRangeWithScores("SET", -3, -1).futureValue should be (LinkedHashSet(
          ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET").futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.del("SET")
      }
    }
  }

  ZRangeByScore.name when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        redis.zRangeByScore("SET", Infinity, Infinity).futureValue should be (empty)
        redis.zRangeByScoreWithScores("SET", Infinity, Infinity).futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zRangeByScore("HASH", Infinity, Infinity).futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zRangeByScoreWithScores("HASH", Infinity, Infinity).futureValue
        }
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        redis.zAdd("SET", ("A", -5))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("D", 3))

        redis.zRangeByScore("SET", Infinity, Infinity).futureValue should be (LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeByScoreWithScores("SET", Infinity, Infinity).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", Infinity, inclusive(0)).futureValue should be (LinkedHashSet("A", "B", "C"))
        redis.zRangeByScoreWithScores("SET", Infinity, inclusive(0)).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0)
        ))

        redis.zRangeByScore("SET", Infinity, exclusive(0)).futureValue should be (LinkedHashSet("A", "B"))
        redis.zRangeByScoreWithScores("SET", Infinity, exclusive(0)).futureValue should be (LinkedHashSet(
          ("A", -5), ("B", -1)
        ))

        redis.zRangeByScore("SET", inclusive(-1), Infinity).futureValue should be (LinkedHashSet("B", "C", "D"))
        redis.zRangeByScoreWithScores("SET", inclusive(-1), Infinity).futureValue should be (LinkedHashSet(
          ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", exclusive(-1), Infinity).futureValue should be (LinkedHashSet("C", "D"))
        redis.zRangeByScoreWithScores("SET", exclusive(-1), Infinity).futureValue should be (LinkedHashSet(
          ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", inclusive(-1), inclusive(0)).futureValue should be (LinkedHashSet("B", "C"))
        redis.zRangeByScoreWithScores("SET", inclusive(-1), inclusive(0)).futureValue should be (LinkedHashSet(
          ("B", -1), ("C", 0)
        ))

        redis.zRangeByScore("SET", exclusive(-1), inclusive(0)).futureValue should be (LinkedHashSet("C"))
        redis.zRangeByScoreWithScores("SET", exclusive(-1), inclusive(0)).futureValue should be (LinkedHashSet(
          ("C", 0)
        ))

        redis.zRangeByScore("SET", exclusive(-1), exclusive(0)).futureValue should be (empty)
        redis.zRangeByScoreWithScores("SET", exclusive(-1), exclusive(0)).futureValue should be (empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          redis.zRangeByScore("SET", Infinity, Infinity, Some((0, 3))).futureValue should be (LinkedHashSet(
            "A", "B", "C"
          ))
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 3))).futureValue should be (
            LinkedHashSet(("A", -5), ("B", -1), ("C", 0))
          )

          redis.zRangeByScore("SET", Infinity, Infinity, Some((1, 4))).futureValue should be (LinkedHashSet(
            "B", "C", "D")
          )
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((1, 4))).futureValue should be (
            LinkedHashSet(("B", -1), ("C", 0), ("D", 3))
          )

          redis.zRangeByScore("SET", Infinity, Infinity, Some((0, 0))).futureValue should be (empty)
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 0))).futureValue should be (empty)

          redis.del("SET")
        }
    }
  }

  ZRank.name when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        redis.zRank("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRank("HASH", "hello").futureValue }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRank("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRank("SET", "C").futureValue should be (Some(2))
        redis.del("SET")
      }
    }
  }

  ZRem.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRem("SET", "A").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRem("HASH", "A").futureValue }
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRem("SET", "B").futureValue should be (0)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "C"))
      }
    }
    "the sorted set contains the element" should {
      "remove the element" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRem("SET", "B").futureValue should be (1)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "C"))
        redis.del("SET")
      }
    }
  }

  "%s-2.4".format(ZRem).name when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRem("SET", "A", "B").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRem("HASH", "A", "B").futureValue }
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRem("SET", "B", "D", "E").futureValue should be (0)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "C"))
      }
    }
    "the sorted set contains some elements" should {
      "remove the elements" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("D", 4))
        redis.zRem("SET", "B", "D", "E").futureValue should be (2)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "C"))
        redis.del("SET")
      }
    }
  }

  ZRemRangeByRank.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        redis.zRemRangeByRank("SET", 0, -1).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRemRangeByRank("HASH", 0, -1).futureValue }
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified range" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("C", 3))
        redis.zAdd("SET", ("D", 4))
        redis.zAdd("SET", ("E", 5))

        redis.zRemRangeByRank("SET", 5, 6).futureValue should be (0)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "B", "C", "D", "E"))
        redis.zRemRangeByRank("SET", 0, 0).futureValue should be (1)
        redis.zRange("SET").futureValue should be (LinkedHashSet("B", "C", "D", "E"))
        redis.zRemRangeByRank("SET", 1, 2).futureValue should be (2)
        redis.zRange("SET").futureValue should be (LinkedHashSet("B", "E"))
        redis.zRemRangeByRank("SET", 0, -1).futureValue should be (2)
        redis.zRange("SET").futureValue should be (empty)
      }
    }
  }

  ZRemRangeByScore.name when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRemRangeByScore("SET", Infinity, Infinity).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zRemRangeByScore("HASH", Infinity, Infinity).futureValue
        }
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified score range" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("C", 3))
        redis.zAdd("SET", ("D", 4))
        redis.zAdd("SET", ("E", 5))

        redis.zRemRangeByScore("SET", exclusive(5), inclusive(7)).futureValue should be (0)
        redis.zRange("SET").futureValue should be (LinkedHashSet("A", "B", "C", "D", "E"))
        redis.zRemRangeByScore("SET", inclusive(1), inclusive(1)).futureValue should be (1)
        redis.zRange("SET").futureValue should be (LinkedHashSet("B", "C", "D", "E"))
        redis.zRemRangeByScore("SET", exclusive(2), exclusive(5)).futureValue should be (2)
        redis.zRange("SET").futureValue should be (LinkedHashSet("B", "E"))
        redis.zRemRangeByScore("SET", Infinity, Infinity).futureValue should be (2)
        redis.zRange("SET").futureValue should be (empty)
      }
    }
  }

  ZRevRange.name when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zRevRange("SET").futureValue should be (empty)
        redis.zRevRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRevRange("HASH").futureValue }
        a [RedisErrorResponseException] should be thrownBy { redis.zRevRangeWithScores("HASH").futureValue }
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        redis.zAdd("SET", ("D", 3))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("A", -5))

        redis.zRevRange("SET", 0, 0).futureValue should be (LinkedHashSet("D"))
        redis.zRevRangeWithScores("SET", 0, 0).futureValue should be (LinkedHashSet(("D", 3)))

        redis.zRevRange("SET", 3, 3).futureValue should be (LinkedHashSet("A"))
        redis.zRevRangeWithScores("SET", 3, 3).futureValue should be (LinkedHashSet(("A", -5)))

        redis.zRevRange("SET", 1, 2).futureValue should be (LinkedHashSet("C", "B"))
        redis.zRevRangeWithScores("SET", 1, 2).futureValue should be (LinkedHashSet(
          ("C", 0), ("B", -1)
        ))

        redis.zRevRange("SET", 0, 3).futureValue should be (LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET", 0, 3).futureValue should be (LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET", 0, -1).futureValue should be (LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET", 0, -1).futureValue should be (LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET", 0, -2).futureValue should be (LinkedHashSet("D", "C", "B"))
        redis.zRevRangeWithScores("SET", 0, -2).futureValue should be (LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1)
        ))

        redis.zRevRange("SET", -3, -1).futureValue should be (LinkedHashSet("C", "B", "A"))
        redis.zRevRangeWithScores("SET", -3, -1).futureValue should be (LinkedHashSet(
          ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET").futureValue should be (LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET").futureValue should be (LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.del("SET")
      }
    }
  }

  ZRevRangeByScore.name when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        redis.zRevRangeByScore("SET", Infinity, Infinity).futureValue should be (empty)
        redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity).futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zRevRangeByScore("HASH", Infinity, Infinity).futureValue
        }
        a [RedisErrorResponseException] should be thrownBy { 
          redis.zRevRangeByScoreWithScores("HASH", Infinity, Infinity).futureValue
        }
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        redis.zAdd("SET", ("D", 3))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("A", -5))

        redis.zRevRangeByScore("SET", Infinity, Infinity).map(_.toList.reverse).futureValue should be (List(
          "A", "B", "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity).map(_.toList.reverse).futureValue should be (
          List(("A", -5), ("B", -1), ("C", 0), ("D", 3))
        )

        redis.zRevRangeByScore("SET", inclusive(0), Infinity).map(_.toList.reverse).futureValue should be (List(
          "A", "B", "C"
        ))
        redis.zRevRangeByScoreWithScores("SET", inclusive(0), Infinity).map(
          _.toList.reverse
        ).futureValue should be (List(("A", -5), ("B", -1), ("C", 0)))

        redis.zRevRangeByScore("SET", exclusive(0), Infinity).map(_.toList.reverse).futureValue should be (List(
          "A", "B"
        ))
        redis.zRevRangeByScoreWithScores("SET", exclusive(0), Infinity).map(
          _.toList.reverse
        ).futureValue should be (List(("A", -5), ("B", -1)))

        redis.zRevRangeByScore("SET", Infinity, inclusive(-1)).map(_.toList.reverse).futureValue should be (List(
          "B", "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, inclusive(-1)).map(
          _.toList.reverse
        ).futureValue should be (List(("B", -1), ("C", 0), ("D", 3)))

        redis.zRevRangeByScore("SET", Infinity, exclusive(-1)).map(_.toList.reverse).futureValue should be (List(
          "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, exclusive(-1)).map(
          _.toList.reverse
        ).futureValue should be (List(("C", 0), ("D", 3)))

        redis.zRevRangeByScore("SET", inclusive(0), inclusive(-1)).map(_.toList.reverse).futureValue should be (
          List("B", "C")
        )
        redis.zRevRangeByScoreWithScores(
          "SET", inclusive(0), inclusive(-1)
        ).map(_.toList.reverse).futureValue should be (List(("B", -1), ("C", 0)))

        redis.zRevRangeByScore("SET", inclusive(0), exclusive(-1)).map(_.toList.reverse).futureValue should be (
          List("C")
        )
        redis.zRevRangeByScoreWithScores(
          "SET", inclusive(0), exclusive(-1)
        ).map(_.toList.reverse).futureValue should be (List(("C", 0)))

        redis.zRevRangeByScore("SET", exclusive(0), exclusive(-1)).futureValue should be (empty)
        redis.zRevRangeByScoreWithScores("SET", exclusive(0), exclusive(-1)).futureValue should be (empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((0, 3))).futureValue should be (LinkedHashSet(
            "D", "C", "B"
          ))
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 3))).futureValue should be (
            LinkedHashSet(("D", 3), ("C", 0), ("B", -1))
          )

          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((1, 4))).futureValue should be (LinkedHashSet(
            "C", "B", "A"
          ))
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((1, 4))).futureValue should be (
            LinkedHashSet(("C", 0), ("B", -1), ("A", -5))
          )

          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((0, 0))).futureValue should be (empty)
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 0))).futureValue should be (empty)

          redis.del("SET")
        }
    }
  }

  ZRevRank.name when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        redis.zRevRank("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zRevRank("HASH", "hello").futureValue }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRevRank("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRevRank("SET", "C").futureValue should be (Some(0))
        redis.del("SET")
      }
    }
  }

  ZScore.name when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zScore("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zScore("HASH", "A").futureValue }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zScore("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct score" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zScore("SET", "A").futureValue should be (Some(1))
        redis.zScore("SET", "B").futureValue should be (Some(2.0))
        redis.zScore("SET", "C").futureValue should be (Some(3.0))
        redis.del("SET")
      }
    }
  }
  
  ZScan.name when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = redis.zScan[String]("NONEXISTENTKEY")(0).futureValue
        next should be (0)
        set should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy { redis.zScan[String]("HASH")(0).futureValue }
      }
    }
    "the sorted set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          redis.zAdd("SSET", ("value" + i, i)).futureValue
        }
        val (next, set) = redis.zScan[String]("SSET")(0).futureValue
        next should be (0)
        set should (
          contain(("value1", 1.0)) and
          contain(("value2", 2.0)) and
          contain(("value3", 3.0)) and
          contain(("value4", 4.0)) and
          contain(("value5", 5.0))
        )
        for (i <- 1 to 10) {
          redis.zAdd("SSET", ("foo" + i, 5 + i)).futureValue
        }
      }
    }
    "the sorted set contains 15 elements" should {
      val full = LinkedHashSet.newBuilder[(String, Double)]
      for (i <- 1 to 5) {
        full += (("value" + i, i))
      }
      for (i <- 1 to 10) {
        full += (("foo" + i, 5 + i))
      }
      val fullSet = full.result()
      
      Given("that no pattern is set")
      "return all elements" taggedAs (V280) in {
        val elements = LinkedHashSet.newBuilder[(String, Double)]
        var cursor = 0L
        do {
          val (next, set) = redis.zScan[String]("SSET")(cursor).futureValue
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result().futureValue should be (fullSet)
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = LinkedHashSet.newBuilder[(String, Double)]
        var cursor = 0L
        do {
          val (next, set) = redis.zScan[String]("SSET")(cursor, matchOpt = Some("foo*")).futureValue
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result().futureValue should be (fullSet.filter {
          case (value, score) => value.startsWith("foo")
        })
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs (V280) in {
        val elements = LinkedHashSet.newBuilder[(String, Double)]
        var cursor = 0L
        do {
          val (next, set) = redis.zScan[String]("SSET")(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).futureValue
          set.size should be (10)
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result().futureValue should be (fullSet.filter {
          case (value, score) => value.startsWith("foo")
        })
      }
    }
  }

  override def afterAll() {
    redis.flushDb().futureValue
    redis.quit()
  }

}
