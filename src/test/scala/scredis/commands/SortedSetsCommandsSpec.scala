/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package scredis.commands

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.MustMatchers._

import scredis.Redis
import scredis.Score._
import scredis.Aggregate._
import scredis.exceptions.RedisCommandException
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

  ZAdd when {
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V120) in {
        redis.zAdd("SET", (SomeValue, 1)) must be(1)
        redis.zRange("SET") must contain(SomeValue)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zAdd("HASH", ("hello", 1)) } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "add the provided member only if it is not already contained in the " +
        "sorted set" taggedAs (V120) in {
          redis.zAdd("SET", (SomeValue, 0)) must be(0)
          redis.zAdd("SET", ("A", -1.3))
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", -1.3), (SomeValue, 0)))
          redis.del("SET")
        }
    }
  }

  "%s-2.4".format(ZAdd) when {
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1), ("B", 1.5)) must be(2)
        redis.zRange("SET") must (contain("A") and contain("B"))
        redis.del("SET")
        redis.zAddFromMap("SET", Map("A" -> 1, "B" -> 1.5)) must be(2)
      }
    }
    "providing an empty map" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zAddFromMap("SET", Map()) } must produce[RedisCommandException]
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating {
          redis.zAddFromMap("HASH", Map("hello" -> 1, "asd" -> 2))
        } must produce[RedisCommandException]
        evaluating {
          redis.zAdd("HASH", ("hello", 1), ("asd", 2))
        } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "add the provided members only if they are not already contained " +
        "in the sorted set" taggedAs (V120) in {
          redis.zAdd("SET", ("A", 2.5), ("B", 3.8)) must be(0)
          redis.zAddFromMap("SET", Map("C" -> -1.3, "D" -> -2.6)) must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("D", -2.6), ("C", -1.3), ("A", 2.5), ("B", 3.8)
          ))
          redis.del("SET")
        }
    }
  }

  ZCard when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zCard("SET") must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zCard("HASH") } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET", ("B", 0))
        redis.zAdd("SET", ("C", 0))
        redis.zCard("SET") must be(3)
        redis.del("SET")
      }
    }
  }

  ZCount when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        redis.zCount("SET", Infinity, Infinity) must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating {
          redis.zCount("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V200) in {
        redis.zAdd("SET", ("A", -1.5))
        redis.zAdd("SET", ("B", 0.4))
        redis.zAdd("SET", ("C", 1.6))
        redis.zAdd("SET", ("D", 3))
        redis.zCount("SET", inclusive(-3), inclusive(-3)) must be(0)
        redis.zCount("SET", inclusive(4), inclusive(4)) must be(0)
        redis.zCount("SET", inclusive(-1.5), inclusive(3)) must be(4)
        redis.zCount("SET", exclusive(-1.5), exclusive(3)) must be(2)
        redis.zCount("SET", inclusive(-1.5), exclusive(3)) must be(3)
        redis.zCount("SET", exclusive(-1.5), inclusive(3)) must be(3)
        redis.zCount("SET", exclusive(0), exclusive(0.5)) must be(1)
        redis.zCount("SET", inclusive(0.5), Infinity) must be(2)
        redis.zCount("SET", Infinity, inclusive(0.5)) must be(2)
        redis.zCount("SET", Infinity, Infinity) must be(4)
        redis.del("SET")
      }
    }
  }

  ZIncrBy when {
    "the key does not exist" should {
      "create a sorted set, add the member and increment the score starting " +
        "from zero" taggedAs (V120) in {
          redis.zIncrBy("SET", "A", 1.5) must be(1.5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 1.5)))
        }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zIncrBy("HASH", "A", 1.5) } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "increment the score of the given member" taggedAs (V120) in {
        redis.zIncrBy("SET", "A", 1.5) must be(3.0)
        redis.zIncrBy("SET", "A", -0.5) must be(2.5)
        redis.zIncrBy("SET", "B", -0.7) must be(-0.7)
        redis.zRangeWithScores("SET") must be(LinkedHashSet(("B", -0.7), ("A", 2.5)))
        redis.del("SET")
      }
    }
  }

  ZInterStore when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        redis.zInterStore("SET")("SET1", "SET2") must be(0)
        redis.zRangeWithScores("SET") must be('empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET1", ("A", 0))
        redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 0)))
        redis.zInterStore("SET")("SET1", "SET2", "SET3") must be(0)
        redis.zRangeWithScores("SET") must be('empty)
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating { redis.zInterStore("SET")("HASH") } must produce[RedisCommandException]
        evaluating { redis.zInterStore("SET")("HASH", "SET2") } must produce[RedisCommandException]
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

          redis.zInterStore("SET")("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zInterStore("SET")("SET1", "SET2") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", 2.3 + 5.5)))

          redis.zInterStore("SET")("SET1", "SET3") must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0 + (-1.0)), ("C", 2.3 + (-2.13))))

          redis.zInterStore("SET")("SET1", "SET2", "SET3") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", 2.3 + 5.5 + (-2.13))))
        }
      Given("that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zInterStore("SET", Min)("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)))

          redis.zInterStore("SET", Min)("SET1", "SET2") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", min(2.3, 5.5))))

          redis.zInterStore("SET", Min)("SET1", "SET3") must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", -2.13), ("A", -1)))

          redis.zInterStore("SET", Min)("SET1", "SET2", "SET3") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", min(min(2.3, 5.5), -2.13))))
        }
      Given("that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zInterStore("SET", Max)("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zInterStore("SET", Max)("SET1", "SET2") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", max(2.3, 5.5))))

          redis.zInterStore("SET", Max)("SET1", "SET3") must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 0), ("C", 2.3)))

          redis.zInterStore("SET", Max)("SET1", "SET2", "SET3") must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", max(max(2.3, 5.5), -2.13))))
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the intersection between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 3 * 0), ("B", 3 * 1.7), ("C", 3 * 2.3), ("D", 3 * 4.41)
          ))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET2", 2)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", 2.3 + 2 * 5.5)))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET3", 2)) must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0 + 2 * (-1.0)), ("C", 2.3 + 2 * (-2.13))
          ))

          redis.zInterStoreWeighted("SET")(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("C", 2.3 + 2 * 5.5 + (-1) * (-2.13))
          ))
        }
      Given("some custom weights and that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", min(2.3, 2 * 5.5))))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET3", 2)) must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", -4.26), ("A", -2)))

          redis.zInterStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("C", min(min(2.3, 2 * 5.5), (-1) * (-2.13)))
          ))
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("C", max(2.3, 2 * 5.5))))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET3", 2)) must be(2)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 0), ("C", 2.3)))

          redis.zInterStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(1)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("C", max(max(2.3, 2 * 5.5), (-1) * (-2.13)))
          ))

          redis.del("SET1", "SET2", "SET3")
        }
    }
  }

  ZUnionStore when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        redis.zUnionStore("SET")("SET1", "SET2") must be(0)
        redis.zRangeWithScores("SET") must be('empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 0))
        redis.zAdd("SET1", ("A", 0))
        redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 0)))
        redis.zUnionStore("SET")("SET1", "SET2", "SET3") must be(1)
        redis.zRangeWithScores("SET") must be(LinkedHashSet(("A", 0)))
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating { redis.zUnionStore("SET")("HASH") } must produce[RedisCommandException]
        evaluating { redis.zUnionStore("SET")("HASH", "SET2") } must produce[RedisCommandException]
        evaluating { redis.zUnionStore("SET")("SET1", "HASH") } must produce[RedisCommandException]
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

          redis.zUnionStore("SET")("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          ))

          redis.zUnionStore("SET")("SET1", "SET2") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5)
          ))

          redis.zUnionStore("SET")("SET1", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("A", -1), ("C", 2.3 - 2.13), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStore("SET")("SET1", "SET2", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("A", -1), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5 - 2.13)
          ))
        }
      Given("that the aggregation function is Min")
      "compute the union between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zUnionStore("SET", Min)("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET2") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Min)("SET1", "SET2", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          ))
        }
      Given("that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          redis.zUnionStore("SET", Max)("SET1", "SET1") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET2") must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStore("SET", Max)("SET1", "SET2", "SET3") must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", -5.56), ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          ))
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the union between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 3 * 0), ("B", 3 * 1.7), ("C", 3 * 2.3), ("D", 3 * 4.41)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET2", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 2 * 5.5)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET3", 2)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", 2 * (-5.56)), ("A", 2 * (-1)), ("C", 2.3 + 2 * (-2.13)), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET")(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
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
          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET3", 2)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", 2 * (-5.56)), ("C", -4.26), ("A", -2), ("B", 1.7), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Min)(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("C", 2.13), ("D", 4.41), ("E", 5.56)
          ))
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET1", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 3.4), ("C", 4.6), ("D", 8.82)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2)) must be(4)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 11)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET3", 2)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("E", 2 * (-5.56)), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          ))

          redis.zUnionStoreWeighted("SET", Max)(("SET1", 1), ("SET2", 2), ("SET3", -1)) must be(5)
          redis.zRangeWithScores("SET") must be(LinkedHashSet(
            ("A", 1), ("B", 1.7), ("D", 4.41), ("E", 5.56), ("C", 11)
          ))
          redis.del("SET", "SET1", "SET2", "SET3")
        }
    }
  }

  ZRange when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zRange("SET") must be('empty)
        redis.zRangeWithScores("SET") must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zRange("HASH") } must produce[RedisCommandException]
        evaluating { redis.zRangeWithScores("HASH") } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        redis.zAdd("SET", ("A", -5))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("D", 3))

        redis.zRange("SET", 0, 0) must be(LinkedHashSet("A"))
        redis.zRangeWithScores("SET", 0, 0) must be(LinkedHashSet(("A", -5)))

        redis.zRange("SET", 3, 3) must be(LinkedHashSet("D"))
        redis.zRangeWithScores("SET", 3, 3) must be(LinkedHashSet(("D", 3)))

        redis.zRange("SET", 1, 2) must be(LinkedHashSet("B", "C"))
        redis.zRangeWithScores("SET", 1, 2) must be(LinkedHashSet(("B", -1), ("C", 0)))

        redis.zRange("SET", 0, 3) must be(LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET", 0, 3) must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET", 0, -1) must be(LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET", 0, -1) must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET", 0, -2) must be(LinkedHashSet("A", "B", "C"))
        redis.zRangeWithScores("SET", 0, -2) must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0)
        ))

        redis.zRange("SET", -3, -1) must be(LinkedHashSet("B", "C", "D"))
        redis.zRangeWithScores("SET", -3, -1) must be(LinkedHashSet(
          ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRange("SET") must be(LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeWithScores("SET") must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.del("SET")
      }
    }
  }

  ZRangeByScore when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        redis.zRangeByScore("SET", Infinity, Infinity) must be('empty)
        redis.zRangeByScoreWithScores("SET", Infinity, Infinity) must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        evaluating {
          redis.zRangeByScore("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
        evaluating {
          redis.zRangeByScoreWithScores("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        redis.zAdd("SET", ("A", -5))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("D", 3))

        redis.zRangeByScore("SET", Infinity, Infinity) must be(LinkedHashSet("A", "B", "C", "D"))
        redis.zRangeByScoreWithScores("SET", Infinity, Infinity) must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", Infinity, inclusive(0)) must be(LinkedHashSet("A", "B", "C"))
        redis.zRangeByScoreWithScores("SET", Infinity, inclusive(0)) must be(LinkedHashSet(
          ("A", -5), ("B", -1), ("C", 0)
        ))

        redis.zRangeByScore("SET", Infinity, exclusive(0)) must be(LinkedHashSet("A", "B"))
        redis.zRangeByScoreWithScores("SET", Infinity, exclusive(0)) must be(LinkedHashSet(
          ("A", -5), ("B", -1)
        ))

        redis.zRangeByScore("SET", inclusive(-1), Infinity) must be(LinkedHashSet("B", "C", "D"))
        redis.zRangeByScoreWithScores("SET", inclusive(-1), Infinity) must be(LinkedHashSet(
          ("B", -1), ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", exclusive(-1), Infinity) must be(LinkedHashSet("C", "D"))
        redis.zRangeByScoreWithScores("SET", exclusive(-1), Infinity) must be(LinkedHashSet(
          ("C", 0), ("D", 3)
        ))

        redis.zRangeByScore("SET", inclusive(-1), inclusive(0)) must be(LinkedHashSet("B", "C"))
        redis.zRangeByScoreWithScores("SET", inclusive(-1), inclusive(0)) must be(LinkedHashSet(
          ("B", -1), ("C", 0)
        ))

        redis.zRangeByScore("SET", exclusive(-1), inclusive(0)) must be(LinkedHashSet("C"))
        redis.zRangeByScoreWithScores("SET", exclusive(-1), inclusive(0)) must be(LinkedHashSet(
          ("C", 0)
        ))

        redis.zRangeByScore("SET", exclusive(-1), exclusive(0)) must be('empty)
        redis.zRangeByScoreWithScores("SET", exclusive(-1), exclusive(0)) must be('empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          redis.zRangeByScore("SET", Infinity, Infinity, Some((0, 3))) must be(LinkedHashSet(
            "A", "B", "C"
          ))
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 3))) must be(
            LinkedHashSet(("A", -5), ("B", -1), ("C", 0))
          )

          redis.zRangeByScore("SET", Infinity, Infinity, Some((1, 4))) must be(LinkedHashSet(
            "B", "C", "D")
          )
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((1, 4))) must be(
            LinkedHashSet(("B", -1), ("C", 0), ("D", 3))
          )

          redis.zRangeByScore("SET", Infinity, Infinity, Some((0, 0))) must be('empty)
          redis.zRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 0))) must be('empty)

          redis.del("SET")
        }
    }
  }

  ZRank when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        redis.zRank("SET", "A") must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating { redis.zRank("HASH", "hello") } must produce[RedisCommandException]
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRank("SET", "B") must be('empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRank("SET", "C") must be(Some(2))
        redis.del("SET")
      }
    }
  }

  ZRem when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRem("SET", "A") must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zRem("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRem("SET", "B") must be(0)
        redis.zRange("SET") must be(LinkedHashSet("A", "C"))
      }
    }
    "the sorted set contains the element" should {
      "remove the element" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRem("SET", "B") must be(1)
        redis.zRange("SET") must be(LinkedHashSet("A", "C"))
        redis.del("SET")
      }
    }
  }

  "%s-2.4".format(ZRem) when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRem("SET", "A", "B") must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zRem("HASH", "A", "B") } must produce[RedisCommandException]
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRem("SET", "B", "D", "E") must be(0)
        redis.zRange("SET") must be(LinkedHashSet("A", "C"))
      }
    }
    "the sorted set contains some elements" should {
      "remove the elements" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("D", 4))
        redis.zRem("SET", "B", "D", "E") must be(2)
        redis.zRange("SET") must be(LinkedHashSet("A", "C"))
        redis.del("SET")
      }
    }
  }

  ZRemRangeByRank when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        redis.zRemRangeByRank("SET", 0, -1) must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating { redis.zRemRangeByRank("HASH", 0, -1) } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified range" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("C", 3))
        redis.zAdd("SET", ("D", 4))
        redis.zAdd("SET", ("E", 5))

        redis.zRemRangeByRank("SET", 5, 6) must be(0)
        redis.zRange("SET") must be(LinkedHashSet("A", "B", "C", "D", "E"))
        redis.zRemRangeByRank("SET", 0, 0) must be(1)
        redis.zRange("SET") must be(LinkedHashSet("B", "C", "D", "E"))
        redis.zRemRangeByRank("SET", 1, 2) must be(2)
        redis.zRange("SET") must be(LinkedHashSet("B", "E"))
        redis.zRemRangeByRank("SET", 0, -1) must be(2)
        redis.zRange("SET") must be('empty)
      }
    }
  }

  ZRemRangeByScore when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        redis.zRemRangeByScore("SET", Infinity, Infinity) must be(0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating {
          redis.zRemRangeByScore("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified score range" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("B", 2))
        redis.zAdd("SET", ("C", 3))
        redis.zAdd("SET", ("D", 4))
        redis.zAdd("SET", ("E", 5))

        redis.zRemRangeByScore("SET", exclusive(5), inclusive(7)) must be(0)
        redis.zRange("SET") must be(LinkedHashSet("A", "B", "C", "D", "E"))
        redis.zRemRangeByScore("SET", inclusive(1), inclusive(1)) must be(1)
        redis.zRange("SET") must be(LinkedHashSet("B", "C", "D", "E"))
        redis.zRemRangeByScore("SET", exclusive(2), exclusive(5)) must be(2)
        redis.zRange("SET") must be(LinkedHashSet("B", "E"))
        redis.zRemRangeByScore("SET", Infinity, Infinity) must be(2)
        redis.zRange("SET") must be('empty)
      }
    }
  }

  ZRevRange when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zRevRange("SET") must be('empty)
        redis.zRevRangeWithScores("SET") must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zRevRange("HASH") } must produce[RedisCommandException]
        evaluating { redis.zRevRangeWithScores("HASH") } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        redis.zAdd("SET", ("D", 3))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("A", -5))

        redis.zRevRange("SET", 0, 0) must be(LinkedHashSet("D"))
        redis.zRevRangeWithScores("SET", 0, 0) must be(LinkedHashSet(("D", 3)))

        redis.zRevRange("SET", 3, 3) must be(LinkedHashSet("A"))
        redis.zRevRangeWithScores("SET", 3, 3) must be(LinkedHashSet(("A", -5)))

        redis.zRevRange("SET", 1, 2) must be(LinkedHashSet("C", "B"))
        redis.zRevRangeWithScores("SET", 1, 2) must be(LinkedHashSet(
          ("C", 0), ("B", -1)
        ))

        redis.zRevRange("SET", 0, 3) must be(LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET", 0, 3) must be(LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET", 0, -1) must be(LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET", 0, -1) must be(LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET", 0, -2) must be(LinkedHashSet("D", "C", "B"))
        redis.zRevRangeWithScores("SET", 0, -2) must be(LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1)
        ))

        redis.zRevRange("SET", -3, -1) must be(LinkedHashSet("C", "B", "A"))
        redis.zRevRangeWithScores("SET", -3, -1) must be(LinkedHashSet(
          ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.zRevRange("SET") must be(LinkedHashSet("D", "C", "B", "A"))
        redis.zRevRangeWithScores("SET") must be(LinkedHashSet(
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        ))

        redis.del("SET")
      }
    }
  }

  ZRevRangeByScore when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        redis.zRevRangeByScore("SET", Infinity, Infinity) must be('empty)
        redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity) must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        evaluating {
          redis.zRevRangeByScore("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
        evaluating {
          redis.zRevRangeByScoreWithScores("HASH", Infinity, Infinity)
        } must produce[RedisCommandException]
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        redis.zAdd("SET", ("D", 3))
        redis.zAdd("SET", ("C", 0))
        redis.zAdd("SET", ("B", -1))
        redis.zAdd("SET", ("A", -5))

        redis.zRevRangeByScore("SET", Infinity, Infinity).map(_.toList.reverse) must be(List(
          "A", "B", "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity).map(_.toList.reverse) must be(
          List(("A", -5), ("B", -1), ("C", 0), ("D", 3))
        )

        redis.zRevRangeByScore("SET", inclusive(0), Infinity).map(_.toList.reverse) must be(List(
          "A", "B", "C"
        ))
        redis.zRevRangeByScoreWithScores("SET", inclusive(0), Infinity).map(
          _.toList.reverse
        ) must be(List(("A", -5), ("B", -1), ("C", 0)))

        redis.zRevRangeByScore("SET", exclusive(0), Infinity).map(_.toList.reverse) must be(List(
          "A", "B"
        ))
        redis.zRevRangeByScoreWithScores("SET", exclusive(0), Infinity).map(
          _.toList.reverse
        ) must be(List(("A", -5), ("B", -1)))

        redis.zRevRangeByScore("SET", Infinity, inclusive(-1)).map(_.toList.reverse) must be(List(
          "B", "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, inclusive(-1)).map(
          _.toList.reverse
        ) must be(List(("B", -1), ("C", 0), ("D", 3)))

        redis.zRevRangeByScore("SET", Infinity, exclusive(-1)).map(_.toList.reverse) must be(List(
          "C", "D"
        ))
        redis.zRevRangeByScoreWithScores("SET", Infinity, exclusive(-1)).map(
          _.toList.reverse
        ) must be(List(("C", 0), ("D", 3)))

        redis.zRevRangeByScore("SET", inclusive(0), inclusive(-1)).map(_.toList.reverse) must be(
          List("B", "C")
        )
        redis.zRevRangeByScoreWithScores(
          "SET", inclusive(0), inclusive(-1)
        ).map(_.toList.reverse) must be(List(("B", -1), ("C", 0)))

        redis.zRevRangeByScore("SET", inclusive(0), exclusive(-1)).map(_.toList.reverse) must be(
          List("C")
        )
        redis.zRevRangeByScoreWithScores(
          "SET", inclusive(0), exclusive(-1)
        ).map(_.toList.reverse) must be(List(("C", 0)))

        redis.zRevRangeByScore("SET", exclusive(0), exclusive(-1)) must be('empty)
        redis.zRevRangeByScoreWithScores("SET", exclusive(0), exclusive(-1)) must be('empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((0, 3))) must be(LinkedHashSet(
            "D", "C", "B"
          ))
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 3))) must be(
            LinkedHashSet(("D", 3), ("C", 0), ("B", -1))
          )

          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((1, 4))) must be(LinkedHashSet(
            "C", "B", "A"
          ))
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((1, 4))) must be(
            LinkedHashSet(("C", 0), ("B", -1), ("A", -5))
          )

          redis.zRevRangeByScore("SET", Infinity, Infinity, Some((0, 0))) must be('empty)
          redis.zRevRangeByScoreWithScores("SET", Infinity, Infinity, Some((0, 0))) must be('empty)

          redis.del("SET")
        }
    }
  }

  ZRevRank when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        redis.zRevRank("SET", "A") must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        evaluating { redis.zRevRank("HASH", "hello") } must produce[RedisCommandException]
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zRevRank("SET", "B") must be('empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        redis.zAdd("SET", ("B", 2))
        redis.zRevRank("SET", "C") must be(Some(0))
        redis.del("SET")
      }
    }
  }

  ZScore when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        redis.zScore("SET", "A") must be('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        evaluating { redis.zScore("HASH", "A") } must produce[RedisCommandException]
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V120) in {
        redis.zAdd("SET", ("A", 1))
        redis.zAdd("SET", ("C", 3))
        redis.zScore("SET", "B") must be('empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct score" taggedAs (V120) in {
        redis.zAdd("SET", ("B", 2))
        redis.zScore("SET", "A") must be(Some(1))
        redis.zScore("SET", "B") must be(Some(2.0))
        redis.zScore("SET", "C") must be(Some(3.0))
        redis.del("SET")
      }
    }
  }
  
  ZScan when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = redis.zScan[String]("NONEXISTENTKEY")(0).!
        next must be(0)
        set must be ('empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V280) in {
        evaluating { redis.zScan[String]("HASH")(0) } must produce[RedisCommandException]
      }
    }
    "the sorted set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          redis.zAdd("SSET", ("value" + i, i)).!
        }
        val (next, set) = redis.zScan[String]("SSET")(0).!
        next must be(0)
        set must (
          contain(("value1", 1.0)) and
          contain(("value2", 2.0)) and
          contain(("value3", 3.0)) and
          contain(("value4", 4.0)) and
          contain(("value5", 5.0))
        )
        for (i <- 1 to 10) {
          redis.zAdd("SSET", ("foo" + i, 5 + i)).!
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
          val (next, set) = redis.zScan[String]("SSET")(cursor).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result() must be(fullSet)
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = LinkedHashSet.newBuilder[(String, Double)]
        var cursor = 0L
        do {
          val (next, set) = redis.zScan[String]("SSET")(cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result() must be(fullSet.filter {
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
          ).!
          set.size must be(10)
          elements ++= set
          cursor = next
        }
        while (cursor > 0)
        elements.result() must be(fullSet.filter {
          case (value, score) => value.startsWith("foo")
        })
      }
    }
  }

  override def afterAll() {
    redis.flushDb().!
    redis.quit()
  }

}
