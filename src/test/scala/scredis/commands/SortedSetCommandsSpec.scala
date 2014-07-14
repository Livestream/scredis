package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.SortedSetRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.LinkedHashSet
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class SortedSetCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"

  override def beforeAll(): Unit = {
    client.hSet("HASH", "FIELD", SomeValue).!
  }

  ZAdd.toString when {
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zAdd("HASH", "hello", 1).!
        }
      }
    }
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V120) in {
        client.zAdd("SET", SomeValue, Score.MinusInfinity).futureValue should be (true)
        client.zRangeWithScores(
          "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          (SomeValue, Score.MinusInfinity)
        )
      }
    }
    "the sorted set contains some elements" should {
      "add the provided member only if it is not already contained in the " +
        "sorted set" taggedAs (V120) in {
          client.zAdd("SET", SomeValue, Score.PlusInfinity).futureValue should be (false)
          client.zAdd("SET", "A", -1.3).futureValue should be (true)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", Score.Value(-1.3)), (SomeValue, Score.PlusInfinity)
          )
          client.del("SET")
        }
    }
  }

  s"${ZAdd.toString}-2.4" when {
    "providing an empty map" should {
      "return 0" taggedAs (V240) in {
        client.zAdd("SET", Map.empty[String, Score]).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zAdd("HASH", Map("hello" -> 1, "asd" -> 2)).!
        }
      }
    }
    "the key does not exist" should {
      "create a sorted set and add the member to it" taggedAs (V240) in {
        client.zAdd("SET", Map("A" -> 1, "B" -> 1.5)).futureValue should be (2)
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", Score.Value(1)), ("B", Score.Value(1.5))
        )
      }
    }
    "the sorted set contains some elements" should {
      "add the provided members only if they are not already contained " +
        "in the sorted set" taggedAs (V240) in {
          client.zAdd("SET", Map("A" -> 2.5, "B" -> 3.8)).futureValue should be (0)
          client.zAdd("SET", Map("C" -> -1.3, "D" -> -2.6)).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("D", -2.6), ("C", -1.3), ("A", 2.5), ("B", 3.8)
          )
          client.del("SET")
        }
    }
  }

  ZCard.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        client.zCard("SET").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zCard("HASH").!
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V120) in {
        client.zAdd("SET", "A", 0)
        client.zAdd("SET", "B", 0)
        client.zAdd("SET", "C", 0)
        client.zCard("SET").futureValue should be (3)
        client.del("SET")
      }
    }
  }

  ZCount.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.zCount(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zCount("HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity).!
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V200) in {
        client.zAdd("SET", "A", -1.5)
        client.zAdd("SET", "B", 0.4)
        client.zAdd("SET", "C", 1.6)
        client.zAdd("SET", "D", 3)
        
        client.zCount(
          "SET", ScoreLimit.Inclusive(-3), ScoreLimit.Inclusive(-3)
        ).futureValue should be (0)
        
        client.zCount(
          "SET", ScoreLimit.Inclusive(4), ScoreLimit.Inclusive(4)
        ).futureValue should be (0)
        
        client.zCount(
          "SET", ScoreLimit.Inclusive(-1.5), ScoreLimit.Inclusive(3)
        ).futureValue should be (4)
        
        client.zCount(
          "SET", ScoreLimit.Exclusive(-1.5), ScoreLimit.Exclusive(3)
        ).futureValue should be (2)
        
        client.zCount(
          "SET", ScoreLimit.Inclusive(-1.5), ScoreLimit.Exclusive(3)
        ).futureValue should be (3)
        
        client.zCount(
          "SET", ScoreLimit.Exclusive(-1.5), ScoreLimit.Inclusive(3)
        ).futureValue should be (3)
        
        client.zCount(
          "SET", ScoreLimit.Exclusive(0), ScoreLimit.Exclusive(0.5)
        ).futureValue should be (1)
        
        client.zCount(
          "SET", ScoreLimit.Inclusive(0.5), ScoreLimit.PlusInfinity
        ).futureValue should be (2)
        
        client.zCount(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.Inclusive(0.5)
        ).futureValue should be (2)
        
        client.zCount(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (4)
        
        client.zCount(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.MinusInfinity
        ).futureValue should be (0)
        
        client.zCount(
          "SET", ScoreLimit.PlusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (0)
        
        client.del("SET")
      }
    }
  }

  ZIncrBy.toString when {
    "the key does not exist" should {
      "create a sorted set, add the member and increment the score starting " +
        "from zero" taggedAs (V120) in {
          client.zIncrBy("SET", "A", 1.5).futureValue should be (1.5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 1.5)
          )
        }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zIncrBy("HASH", "A", 1.5).!
        }
      }
    }
    "the sorted set contains some elements" should {
      "increment the score of the given member" taggedAs (V120) in {
        client.zIncrBy("SET", "A", 1.5).futureValue should be (3.0)
        client.zIncrBy("SET", "A", -0.5).futureValue should be (2.5)
        client.zIncrBy("SET", "B", -0.7).futureValue should be (-0.7)
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -0.7), ("A", 2.5)
        )
        client.del("SET")
      }
    }
  }

  ZInterStore.toString when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        client.zInterStore("SET", Seq("SET1", "SET2")).futureValue should be (0)
        client.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        client.zAdd("SET", "A", 0)
        client.zAdd("SET1", "A", 0)
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", 0)
        )
        client.zInterStore("SET", Seq("SET1", "SET2", "SET3")).futureValue should be (0)
        client.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zInterStore("SET", Seq("HASH")).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.zInterStore("SET", Seq("HASH", "SET2")).!
        }
      }
    }
    "the sorted sets contain some elements" should {
      Given("that the aggregation function is Sum")
      "compute the intersection between them, aggregate the scores with Sum and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zAdd("SET1", "B", 1.7)
          client.zAdd("SET1", "C", 2.3)
          client.zAdd("SET1", "D", 4.41)

          client.zAdd("SET2", "C", 5.5)

          client.zAdd("SET3", "A", -1.0)
          client.zAdd("SET3", "C", -2.13)
          client.zAdd("SET3", "E", -5.56)

          client.zInterStore("SET", Seq("SET1", "SET1")).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          )
          
          client.zInterStore("SET", Seq("SET1", "SET2")).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", 2.3 + 5.5)
          )

          client.zInterStore("SET", Seq("SET1", "SET3")).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0 + (-1.0)), ("C", 2.3 + (-2.13))
          )

          client.zInterStore("SET", Seq("SET1", "SET2", "SET3")).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", 2.3 + 5.5 + (-2.13))
          )
        }
      Given("that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zInterStore("SET", Seq("SET1", "SET1"), Aggregate.Min).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zInterStore("SET", Seq("SET1", "SET2"), Aggregate.Min).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.min(2.3, 5.5))
          )

          client.zInterStore("SET", Seq("SET1", "SET3"), Aggregate.Min).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", -2.13), ("A", -1)
          )

          client.zInterStore(
            "SET", Seq("SET1", "SET2", "SET3"), Aggregate.Min
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.min(math.min(2.3, 5.5), -2.13))
          )
        }
      Given("that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zInterStore("SET", Seq("SET1", "SET1"), Aggregate.Max).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zInterStore("SET", Seq("SET1", "SET2"), Aggregate.Max).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.max(2.3, 5.5))
          )

          client.zInterStore("SET", Seq("SET1", "SET3"), Aggregate.Max).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("C", 2.3)
          )

          client.zInterStore(
            "SET", Seq("SET1", "SET2", "SET3"), Aggregate.Max
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.max(math.max(2.3, 5.5), -2.13))
          )
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the intersection between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zInterStoreWeighted("SET", Map("SET1" -> 1, "SET2" -> 2)).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", 2.3 + 2 * 5.5)
          )

          client.zInterStoreWeighted("SET", Map("SET1" -> 1, "SET3" -> 2)).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0 + 2 * (-1.0)), ("C", 2.3 + 2 * (-2.13))
          )

          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1)
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", 2.3 + 2 * 5.5 + (-1) * (-2.13))
          )
        }
      Given("some custom weights and that the aggregation function is Min")
      "compute the intersection between them, aggregate the scores with Min by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2), Aggregate.Min
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.min(2.3, 2 * 5.5))
          )

          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET3" -> 2), Aggregate.Min
          ).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", -4.26), ("A", -2)
          )

          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1), Aggregate.Min
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.min(math.min(2.3, 2 * 5.5), (-1) * (-2.13)))
          )
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the intersection between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2), Aggregate.Max
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.max(2.3, 2 * 5.5))
          )

          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET3" -> 2), Aggregate.Max
          ).futureValue should be (2)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("C", 2.3)
          )

          client.zInterStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1), Aggregate.Max
          ).futureValue should be (1)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", math.max(math.max(2.3, 2 * 5.5), (-1) * (-2.13)))
          )

          client.del("SET", "SET1", "SET2", "SET3")
        }
    }
  }
  
  ZLexCount.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V289) in {
        client.zLexCount(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zLexCount(
            "HASH", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the correct cardinality" taggedAs (V289) in {
        client.zAdd("SET", "a", 0)
        client.zAdd("SET", "b", 0)
        client.zAdd("SET", "c", 0)
        client.zAdd("SET", "d", 0)
        client.zAdd("SET", "e", 0)
        client.zAdd("SET", "f", 0)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Inclusive("1"), LexicalScoreLimit.Inclusive("1")
        ).futureValue should be (0)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Inclusive("a"), LexicalScoreLimit.Inclusive("a")
        ).futureValue should be (1)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Inclusive("a"), LexicalScoreLimit.Inclusive("c")
        ).futureValue should be (3)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Exclusive("a"), LexicalScoreLimit.Exclusive("c")
        ).futureValue should be (1)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Inclusive("a"), LexicalScoreLimit.Exclusive("f")
        ).futureValue should be (5)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Exclusive("a"), LexicalScoreLimit.Exclusive("f")
        ).futureValue should be (4)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.Inclusive("d"), LexicalScoreLimit.PlusInfinity
        ).futureValue should be (3)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.Inclusive("c")
        ).futureValue should be (3)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (6)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.MinusInfinity
        ).futureValue should be (0)
        
        client.zLexCount(
          "SET", LexicalScoreLimit.PlusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (0)
        
        client.del("SET")
      }
    }
  }

  ZRange.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V120) in {
        client.zRange("SET").futureValue should be (empty)
        client.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRange("HASH").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.zRangeWithScores("HASH").!
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        client.zAdd("SET", "A", -5)
        client.zAdd("SET", "B", -1)
        client.zAdd("SET", "C", 0)
        client.zAdd("SET", "D", 3)

        client.zRange("SET", 0, 0).futureValue should contain theSameElementsInOrderAs List("A")
        client.zRangeWithScores(
          "SET", 0, 0
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5)
        )

        client.zRange("SET", 3, 3).futureValue should contain theSameElementsInOrderAs List("D")
        client.zRangeWithScores(
          "SET", 3, 3
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("D", 3)
        )

        client.zRange("SET", 1, 2).futureValue should contain theSameElementsInOrderAs List(
          "B", "C"
        )
        client.zRangeWithScores(
          "SET", 1, 2
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0)
        )

        client.zRange(
          "SET", 0, 3
        ).futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D"
        )
        client.zRangeWithScores(
          "SET", 0, 3
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        )
        
        client.zRange("SET", 0, -1).futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D"
        )
        client.zRangeWithScores(
          "SET", 0, -1
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRange("SET", 0, -2).futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
        client.zRangeWithScores(
          "SET", 0, -2
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0)
        )

        client.zRange("SET", -3, -1).futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D"
        )
        client.zRangeWithScores(
          "SET", -3, -1
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D"
        )
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        )

        client.del("SET")
      }
    }
  }
  
  ZRangeByLex.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V289) in {
        client.zRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRangeByLex(
            "HASH", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V289) in {
        client.zAdd("SET", "A", 0)
        client.zAdd("SET", "B", 0)
        client.zAdd("SET", "C", 0)
        client.zAdd("SET", "D", 0)

        client.zRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("A", "B", "C", "D")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.Inclusive("C")
        ).futureValue should contain theSameElementsInOrderAs List("A", "B", "C")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.Exclusive("C")
        ).futureValue should contain theSameElementsInOrderAs List("A", "B")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.Inclusive("B"), LexicalScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("B", "C", "D")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.Exclusive("B"), LexicalScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("C", "D")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.Inclusive("B"), LexicalScoreLimit.Inclusive("C")
        ).futureValue should contain theSameElementsInOrderAs List("B", "C")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.Exclusive("B"), LexicalScoreLimit.Inclusive("C")
        ).futureValue should contain theSameElementsInOrderAs List("C")

        client.zRangeByLex(
          "SET", LexicalScoreLimit.Exclusive("A"), LexicalScoreLimit.Exclusive("B")
        ).futureValue should be (empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V289) in {
          client.zRangeByLex(
            "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity, Some((0, 3))
          ).futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C"
          )

          client.zRangeByLex(
            "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity, Some((1, 4))
          ).futureValue should contain theSameElementsInOrderAs List(
            "B", "C", "D"
          )

          client.zRangeByLex(
            "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity, Some((0, 0))
          ).futureValue should be (empty)

          client.del("SET")
        }
    }
  }

  ZRangeByScore.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        client.zRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (empty)
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRangeByScore(
            "HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
          ).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRangeByScoreWithScores(
            "HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        client.zAdd("SET", "A", -5)
        client.zAdd("SET", "B", -1)
        client.zAdd("SET", "C", 0)
        client.zAdd("SET", "D", 3)

        client.zRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("A", "B", "C", "D")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List("A", "B", "C")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.Exclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List("A", "B")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.Exclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.Inclusive(-1), ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("B", "C", "D")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.Inclusive(-1), ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List("C", "D")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.PlusInfinity
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0), ("D", 3)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.Inclusive(-1), ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List("B", "C")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.Inclusive(-1), ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List("C")
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.Inclusive(0)
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0)
        )

        client.zRangeByScore(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.Exclusive(0)
        ).futureValue should be (empty)
        client.zRangeByScoreWithScores(
          "SET", ScoreLimit.Exclusive(-1), ScoreLimit.Exclusive(0)
        ).futureValue should be (empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          client.zRangeByScore(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((0, 3))
          ).futureValue should contain theSameElementsInOrderAs List(
            "A", "B", "C"
          )
          client.zRangeByScoreWithScores(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((0, 3))
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", -5), ("B", -1), ("C", 0)
          )

          client.zRangeByScore(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((1, 4))
          ).futureValue should contain theSameElementsInOrderAs List(
            "B", "C", "D"
          )
          client.zRangeByScoreWithScores(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((1, 4))
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("B", -1), ("C", 0), ("D", 3)
          )

          client.zRangeByScore(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((0, 0))
          ).futureValue should be (empty)
          client.zRangeByScoreWithScores(
            "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity, Some((0, 0))
          ).futureValue should be (empty)

          client.del("SET")
        }
    }
  }

  ZRank.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.zRank("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRank("HASH", "hello").!
        }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "C", 3)
        client.zRank("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        client.zAdd("SET", "B", 2)
        client.zRank("SET", "C").futureValue should contain (2)
        client.del("SET")
      }
    }
  }

  ZRem.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        client.zRem("SET", "A").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRem("HASH", "A").!
        }
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V120) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "C", 3)
        client.zRem("SET", "B").futureValue should be (0)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List("A", "C")
      }
    }
    "the sorted set contains the element" should {
      "remove the element" taggedAs (V120) in {
        client.zAdd("SET", "B", 2)
        client.zRem("SET", "B").futureValue should be (1)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List("A", "C")
        client.del("SET")
      }
    }
  }

  s"${ZRem.toString}-2.4" when {
    "the key does not exist" should {
      "return 0" taggedAs (V240) in {
        client.zRem("SET", "A", "B").futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRem("HASH", "A", "B").!
        }
      }
    }
    "the sorted set does not contain the element" should {
      "do nothing and return 0" taggedAs (V240) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "C", 3)
        client.zRem("SET", "B", "D", "E").futureValue should be (0)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List("A", "C")
      }
    }
    "the sorted set contains some elements" should {
      "remove the elements" taggedAs (V240) in {
        client.zAdd("SET", "B", 2)
        client.zAdd("SET", "D", 4)
        client.zRem("SET", "B", "D", "E").futureValue should be (2)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List("A", "C")
        client.del("SET")
      }
    }
  }
  
  ZRemRangeByLex.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V289) in {
        client.zRemRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V289) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRemRangeByLex(
            "HASH", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified range" taggedAs (V289) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "B", 2)
        client.zAdd("SET", "C", 3)
        client.zAdd("SET", "D", 4)
        client.zAdd("SET", "E", 5)

        client.zRemRangeByLex(
          "SET", LexicalScoreLimit.Exclusive("E"), LexicalScoreLimit.PlusInfinity
        ).futureValue should be (0)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "E"
        )
        
        client.zRemRangeByLex(
          "SET", LexicalScoreLimit.Inclusive("A"), LexicalScoreLimit.Inclusive("A")
        ).futureValue should be (1)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D", "E"
        )
        
        client.zRemRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.Exclusive("D")
        ).futureValue should be (2)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "D", "E"
        )
        
        client.zRemRangeByLex(
          "SET", LexicalScoreLimit.MinusInfinity, LexicalScoreLimit.PlusInfinity
        ).futureValue should be (2)
        client.zRange("SET").futureValue should be (empty)
      }
    }
  }

  ZRemRangeByRank.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V200) in {
        client.zRemRangeByRank("SET", 0, -1).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRemRangeByRank("HASH", 0, -1).!
        }
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified range" taggedAs (V200) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "B", 2)
        client.zAdd("SET", "C", 3)
        client.zAdd("SET", "D", 4)
        client.zAdd("SET", "E", 5)

        client.zRemRangeByRank("SET", 5, 6).futureValue should be (0)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "E"
        )
        client.zRemRangeByRank("SET", 0, 0).futureValue should be (1)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D", "E"
        )
        client.zRemRangeByRank("SET", 1, 2).futureValue should be (2)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "B", "E"
        )
        client.zRemRangeByRank("SET", 0, -1).futureValue should be (2)
        client.zRange("SET").futureValue should be (empty)
      }
    }
  }

  ZRemRangeByScore.toString when {
    "the key does not exist" should {
      "return 0" taggedAs (V120) in {
        client.zRemRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (0)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRemRangeByScore(
            "HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some element" should {
      "remove the elements in the specified score range" taggedAs (V120) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "B", 2)
        client.zAdd("SET", "C", 3)
        client.zAdd("SET", "D", 4)
        client.zAdd("SET", "E", 5)

        client.zRemRangeByScore(
          "SET", ScoreLimit.Exclusive(5), ScoreLimit.Inclusive(7)
        ).futureValue should be (0)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D", "E"
        )
        
        client.zRemRangeByScore(
          "SET", ScoreLimit.Inclusive(1), ScoreLimit.Inclusive(1)
        ).futureValue should be (1)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "B", "C", "D", "E"
        )
        
        client.zRemRangeByScore(
          "SET", ScoreLimit.Exclusive(2), ScoreLimit.Exclusive(5)
        ).futureValue should be (2)
        client.zRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "B", "E"
        )
        
        client.zRemRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (2)
        client.zRange("SET").futureValue should be (empty)
      }
    }
  }

  ZRevRange.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        client.zRevRange("SET").futureValue should be (empty)
        client.zRevRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRevRange("HASH").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.zRevRangeWithScores("HASH").!
        }
      }
    }
    "the sorted set contains some elements" should {
      "return the ordered elements in the specified range" taggedAs (V120) in {
        client.zAdd("SET", "D", 3)
        client.zAdd("SET", "C", 0)
        client.zAdd("SET", "B", -1)
        client.zAdd("SET", "A", -5)

        client.zRevRange("SET", 0, 0).futureValue should contain theSameElementsInOrderAs List("D")
        client.zRevRangeWithScores(
          "SET", 0, 0
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](("D", 3))

        client.zRevRange("SET", 3, 3).futureValue should contain theSameElementsInOrderAs List("A")
        client.zRevRangeWithScores(
          "SET", 3, 3
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](("A", -5))

        client.zRevRange("SET", 1, 2).futureValue should contain theSameElementsInOrderAs List(
          "C", "B"
        )
        client.zRevRangeWithScores(
          "SET", 1, 2
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0), ("B", -1)
        )

        client.zRevRange("SET", 0, 3).futureValue should contain theSameElementsInOrderAs List(
          "D", "C", "B", "A"
        )
        client.zRevRangeWithScores(
          "SET", 0, 3
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        )

        client.zRevRange("SET", 0, -1).futureValue should contain theSameElementsInOrderAs List(
          "D", "C", "B", "A"
        )
        client.zRevRangeWithScores(
          "SET", 0, -1
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        )

        client.zRevRange("SET", 0, -2).futureValue should contain theSameElementsInOrderAs List(
          "D", "C", "B"
        )
        client.zRevRangeWithScores(
          "SET", 0, -2
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("D", 3), ("C", 0), ("B", -1)
        )

        client.zRevRange("SET", -3, -1).futureValue should contain theSameElementsInOrderAs List(
          "C", "B", "A"
        )
        client.zRevRangeWithScores(
          "SET", -3, -1
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0), ("B", -1), ("A", -5)
        )

        client.zRevRange("SET").futureValue should contain theSameElementsInOrderAs List(
          "D", "C", "B", "A"
        )
        client.zRevRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("D", 3), ("C", 0), ("B", -1), ("A", -5)
        )

        client.del("SET")
      }
    }
  }

  ZRevRangeByScore.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V220) in {
        client.zRevRangeByScore(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (empty)
        client.zRevRangeByScoreWithScores(
          "SET", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
        ).futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V220) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRevRangeByScore(
            "HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
          ).!
        }
        a [RedisErrorResponseException] should be thrownBy { 
          client.zRevRangeByScoreWithScores(
            "HASH", ScoreLimit.MinusInfinity, ScoreLimit.PlusInfinity
          ).!
        }
      }
    }
    "the sorted set contains some elements" should {
      Given("that no limit is provided")
      "return the ordered elements in the specified score range" taggedAs (V220) in {
        client.zAdd("SET", "D", 3)
        client.zAdd("SET", "C", 0)
        client.zAdd("SET", "B", -1)
        client.zAdd("SET", "A", -5)

        client.zRevRangeByScore(
          "SET", min = ScoreLimit.MinusInfinity, max = ScoreLimit.PlusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "A", "B", "C", "D"
        )
        client.zRevRangeByScoreWithScores(
          "SET", min = ScoreLimit.MinusInfinity, max = ScoreLimit.PlusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.MinusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "A", "B", "C"
        )
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.MinusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1), ("C", 0)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.Exclusive(0), min = ScoreLimit.MinusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "A", "B"
        )
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.Exclusive(0), min = ScoreLimit.MinusInfinity
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", -5), ("B", -1)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.PlusInfinity, min = ScoreLimit.Inclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "B", "C", "D"
        )
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.PlusInfinity, min = ScoreLimit.Inclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0), ("D", 3)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.PlusInfinity, min = ScoreLimit.Exclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "C", "D"
        )
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.PlusInfinity, min = ScoreLimit.Exclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0), ("D", 3)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.Inclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List(
          "B", "C"
        )
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.Inclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("B", -1), ("C", 0)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.Exclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List("C")
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.Inclusive(0), min = ScoreLimit.Exclusive(-1)
        ).futureValue.reverse should contain theSameElementsInOrderAs List[(String, Score)](
          ("C", 0)
        )

        client.zRevRangeByScore(
          "SET", max = ScoreLimit.Exclusive(0), min = ScoreLimit.Exclusive(-1)
        ).futureValue should be (empty)
        client.zRevRangeByScoreWithScores(
          "SET", max = ScoreLimit.Exclusive(0), min = ScoreLimit.Exclusive(-1)
        ).futureValue should be (empty)
      }
      Given("that some limit is provided")
      "return the ordered elements in the specified score range within " +
        "provided limit" taggedAs (V220) in {
          client.zRevRangeByScore(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((0, 3))
          ).futureValue should contain theSameElementsInOrderAs List(
            "D", "C", "B"
          )
          client.zRevRangeByScoreWithScores(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((0, 3))
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("D", 3), ("C", 0), ("B", -1)
          )
          
          client.zRevRangeByScore(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((1, 4))
          ).futureValue should contain theSameElementsInOrderAs List(
            "C", "B", "A"
          )
          client.zRevRangeByScoreWithScores(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((1, 4))
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("C", 0), ("B", -1), ("A", -5)
          )

          client.zRevRangeByScore(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((0, 0))
          ).futureValue should be (empty)
          client.zRevRangeByScoreWithScores(
            "SET",
            max = ScoreLimit.PlusInfinity,
            min = ScoreLimit.MinusInfinity,
            limitOpt = Some((0, 0))
          ).futureValue should be (empty)

          client.del("SET")
        }
    }
  }

  ZRevRank.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V200) in {
        client.zRevRank("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zRevRank("HASH", "hello").!
        }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V200) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "C", 3)
        client.zRevRank("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct index" taggedAs (V200) in {
        client.zAdd("SET", "B", 2)
        client.zRevRank("SET", "C").futureValue should be (Some(0))
        client.del("SET")
      }
    }
  }
  
  ZScan.toString when {
    "the key does not exist" should {
      "return an empty set" taggedAs (V280) in {
        val (next, set) = client.zScan[String]("NONEXISTENTKEY", 0).!
        next should be (0)
        set should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zScan[String]("HASH", 0).!
        }
      }
    }
    "the sorted set contains 5 elements" should {
      "return all elements" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.zAdd("SSET", "value" + i, i)
        }
        val (next, set) = client.zScan[String]("SSET", 0).!
        next should be (0)
        set should contain theSameElementsInOrderAs List[(String, Score)](
          ("value1", 1.0),
          ("value2", 2.0),
          ("value3", 3.0),
          ("value4", 4.0),
          ("value5", 5.0)
        )
        for (i <- 1 to 10) {
          client.zAdd("SSET", "foo" + i, 5 + i)
        }
      }
    }
    "the sorted set contains 15 elements" should {
      val full = ListBuffer[(String, Score)]()
      for (i <- 1 to 5) {
        full += (("value" + i, i))
      }
      for (i <- 1 to 10) {
        full += (("foo" + i, 5 + i))
      }
      val fullList = full.toList
      
      Given("that no pattern is set")
      "return all elements" taggedAs (V280) in {
        val elements = ListBuffer[(String, Score)]()
        var cursor = 0L
        do {
          val (next, set) = client.zScan[String]("SSET", cursor).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsInOrderAs fullList
      }
      Given("that a pattern is set")
      "return all matching elements" taggedAs (V280) in {
        val elements = ListBuffer[(String, Score)]()
        var cursor = 0L
        do {
          val (next, set) = client.zScan[String]("SSET", cursor, matchOpt = Some("foo*")).!
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsInOrderAs fullList.filter {
          case (value, score) => value.startsWith("foo")
        }
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching elements in one iteration" taggedAs (V280) in {
        val elements = ListBuffer[(String, Score)]()
        var cursor = 0L
        do {
          val (next, set) = client.zScan[String](
            "SSET", cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size should be (10)
          elements ++= set
          cursor = next
        } while (cursor > 0)
        elements.toList should contain theSameElementsInOrderAs fullList.filter {
          case (value, score) => value.startsWith("foo")
        }
      }
    }
  }

  ZScore.toString when {
    "the key does not exist" should {
      "return None" taggedAs (V120) in {
        client.zScore("SET", "A").futureValue should be (empty)
      }
    }
    "the key does not contain a sorted set" should {
      "return an error" taggedAs (V120) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zScore("HASH", "A").!
        }
      }
    }
    "the sorted set does not contain the member" should {
      "return None" taggedAs (V120) in {
        client.zAdd("SET", "A", 1)
        client.zAdd("SET", "C", 3)
        client.zAdd("SET", "D", Score.MinusInfinity)
        client.zAdd("SET", "E", Score.PlusInfinity)
        client.zScore("SET", "B").futureValue should be (empty)
      }
    }
    "the sorted set contains the element" should {
      "the correct score" taggedAs (V120) in {
        client.zAdd("SET", "B", 2)
        client.zScore("SET", "A").futureValue should contain (Score.Value(1))
        client.zScore("SET", "B").futureValue should contain (Score.Value(2.0))
        client.zScore("SET", "C").futureValue should contain (Score.Value(3.0))
        client.zScore("SET", "D").futureValue should contain (Score.MinusInfinity)
        client.zScore("SET", "E").futureValue should contain (Score.PlusInfinity)
        client.del("SET")
      }
    }
  }
  
  ZUnionStore.toString when {
    "the keys do not exist" should {
      "do nothing" taggedAs (V200) in {
        client.zUnionStore("SET", Seq("SET1", "SET2")).futureValue should be (0)
        client.zRangeWithScores("SET").futureValue should be (empty)
      }
    }
    "some keys do not exist" should {
      "overwrite the destination sorted set with the empty set" taggedAs (V200) in {
        client.zAdd("SET", "A", 0)
        client.zAdd("SET1", "A", 0)
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", 0)
        )
        client.zUnionStore("SET", Seq("SET1", "SET2", "SET3")).futureValue should be (1)
        client.zRangeWithScores(
          "SET"
        ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
          ("A", 0)
        )
      }
    }
    "at least one of the source key does not contain a sorted set" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.zUnionStore("SET", Seq("HASH")).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.zUnionStore("SET", Seq("HASH", "SET2")).!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.zUnionStore("SET", Seq("SET1", "HASH")).!
        }
      }
    }
    "the sorted sets contain some elements" should {
      Given("that the aggregation function is Sum")
      "compute the union between them, aggregate the scores with Sum and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zAdd("SET1", "B", 1.7)
          client.zAdd("SET1", "C", 2.3)
          client.zAdd("SET1", "D", 4.41)

          client.zAdd("SET2", "C", 5.5)

          client.zAdd("SET3", "A", -1.0)
          client.zAdd("SET3", "C", -2.13)
          client.zAdd("SET3", "E", -5.56)

          client.zUnionStore("SET", Seq("SET1", "SET1")).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 2 * 0), ("B", 2 * 1.7), ("C", 2 * 2.3), ("D", 2 * 4.41)
          )

          client.zUnionStore("SET", Seq("SET1", "SET2")).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5)
          )

          client.zUnionStore("SET", Seq("SET1", "SET3")).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("A", -1), ("C", 2.3 - 2.13), ("B", 1.7), ("D", 4.41)
          )

          client.zUnionStore("SET", Seq("SET1", "SET2", "SET3")).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("A", -1), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 5.5 - 2.13)
          )
        }
      Given("that the aggregation function is Min")
      "compute the union between them, aggregate the scores with Min and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zUnionStore("SET", Seq("SET1", "SET1"), Aggregate.Min).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStore("SET", Seq("SET1", "SET2"), Aggregate.Min).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStore("SET", Seq("SET1", "SET3"), Aggregate.Min).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          )

          client.zUnionStore(
            "SET", Seq("SET1", "SET2", "SET3"), Aggregate.Min
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("C", -2.13), ("A", -1), ("B", 1.7), ("D", 4.41)
          )
        }
      Given("that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max and " +
        "store the result in the destination" taggedAs (V200) in {
          client.zUnionStore("SET", Seq("SET1", "SET1"), Aggregate.Max).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStore("SET", Seq("SET1", "SET2"), Aggregate.Max).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          )

          client.zUnionStore("SET", Seq("SET1", "SET3"), Aggregate.Max).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStore(
            "SET", Seq("SET1", "SET2", "SET3"), Aggregate.Max
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", -5.56), ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 5.5)
          )
        }
      Given("some custom weights and that the aggregation function is Sum")
      "compute the union between them, aggregate the scores with Sum by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zUnionStoreWeighted("SET", Map("SET1" -> 1, "SET2" -> 2)).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 2.3 + 2 * 5.5)
          )

          client.zUnionStoreWeighted("SET", Map("SET1" -> 1, "SET3" -> 2)).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", 2 * (-5.56)), ("A", 2 * (-1)), ("C", 2.3 + 2 * (-2.13)), ("B", 1.7), ("D", 4.41)
          )

          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1)
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 1),
            ("B", 1.7),
            ("D", 4.41),
            ("E", (-1) * (-5.56)),
            ("C", 2.3 + 2 * 5.5 + (-1) * (-2.13))
          )
        }
      Given("some custom weights and that the aggregation function is Min")
      "compute the union between them, aggregate the scores with Min by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2), Aggregate.Min
          ).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET3" -> 2), Aggregate.Min
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", 2 * (-5.56)), ("C", -4.26), ("A", -2), ("B", 1.7), ("D", 4.41)
          )

          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1), Aggregate.Min
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("C", 2.13), ("D", 4.41), ("E", 5.56)
          )
        }
      Given("some custom weights and that the aggregation function is Max")
      "compute the union between them, aggregate the scores with Max by taking the " +
        "weights into account and store the result in the destination" taggedAs (V200) in {
          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2), Aggregate.Max
          ).futureValue should be (4)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 0), ("B", 1.7), ("D", 4.41), ("C", 11)
          )

          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET3" -> 2), Aggregate.Max
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("E", 2 * (-5.56)), ("A", 0), ("B", 1.7), ("C", 2.3), ("D", 4.41)
          )

          client.zUnionStoreWeighted(
            "SET", Map("SET1" -> 1, "SET2" -> 2, "SET3" -> -1), Aggregate.Max
          ).futureValue should be (5)
          client.zRangeWithScores(
            "SET"
          ).futureValue should contain theSameElementsInOrderAs List[(String, Score)](
            ("A", 1), ("B", 1.7), ("D", 4.41), ("E", 5.56), ("C", 11)
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
