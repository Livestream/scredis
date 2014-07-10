package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol._
import scredis.protocol.requests.ScriptingRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class ScriptingCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val NonExistentKey = "NONEXISTENTKEY"
  private val Value = "HelloWorld"
  private val InvalidScript = "return {asdasd"

  private val ScriptAsUnit = "redis.call('SET', KEYS[1], ARGV[1])"
  private val ScriptAsBooleanTrue = """return true"""
  private val ScriptAsBooleanFalse = """return false"""
  private val ScriptAsInteger = """return redis.call('STRLEN', KEYS[1])"""
  private val ScriptAsStatus = "return redis.call('SET', KEYS[1], ARGV[1])"
  private val ScriptAsString = """return redis.call('GET', KEYS[1])"""
  private val ScriptAsEmptyList = """return {}"""
  private val ScriptAsList = """return redis.call('LRANGE', KEYS[1], ARGV[1], ARGV[2])"""
  private val ScriptAsNestedList = """return {KEYS[1], {KEYS[2], {KEYS[4], KEYS[5]}, KEYS[3]}}"""
  private val ScriptAsNestedListWithNils = """return {KEYS[1], {KEYS[2], {nil, nil}, nil}}"""
  
  private implicit val SimpleStringResponseDecoder: Decoder[String] = {
    case SimpleStringResponse(x) => x
  }
  
  private implicit val SimpleStringResponseToUnitDecoder: Decoder[Unit] = {
    case SimpleStringResponse(_) => ()
  }
  
  private implicit val IntegerResponseDecoder: Decoder[Long] = {
    case IntegerResponse(x) => x
  }
  
  private implicit val IntegerResponseToBooleanDecoder: Decoder[Boolean] = {
    case i: IntegerResponse => i.toBoolean
  }
  
  private implicit val BulkStringResponseDecoder: Decoder[Option[String]] = {
    case b: BulkStringResponse => b.parsed[String]
  }
  
  private implicit val ArrayResponseToListOfFlattenedStringDecoder: Decoder[List[String]] = {
    case a: ArrayResponse => a.parsed[String, List] {
      case b: BulkStringResponse => b.flattened[String]
    }
  }
  
  private implicit val NestedArrayResponseDecoder: Decoder[List[Any]] = {
    case a: ArrayResponse => a.parsed[Any, List] {
      case a: ArrayResponse => a.parsed[Any, List] {
        case a: ArrayResponse => a.parsed[Any, List] {
          case b: BulkStringResponse => b.parsed[String]
        }
        case b: BulkStringResponse => b.parsed[String]
      }
      case b: BulkStringResponse => b.parsed[String]
    }
  }

  override def beforeAll(): Unit = {
    client.hSet("HASH", "FIELD", "VALUE")
    client.rPush("LIST", "A")
    client.rPush("LIST", "B")
    client.rPush("LIST", "C").!
  }

  Eval.toString when {
    "the script is invalid" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.eval[Unit, String, String](InvalidScript).!
        }
      }
    }
    "the script is valid but returns an error" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.eval[Long, String, String](ScriptAsInteger, keys = Seq("HASH")).futureValue
        }
      }
    }
    "the script is a valid Lua script" should {
      Given("that the script does not return anything")
      "succeed without returning anything" taggedAs (V260) in {
        client.eval[Unit, String, String](
          ScriptAsUnit, keys = Seq("STR"), args = Seq(Value)
        ).futureValue should be (())
        client.get("STR").futureValue should contain (Value)
      }
      Given("that the script returns true")
      "succeed and return true" taggedAs (V260) in {
        client.eval[Boolean, String, String](ScriptAsBooleanTrue).futureValue should be (true)
      }
      Given("that the script returns false")
      "succeed and return false" taggedAs (V260) in {
        client.eval[Boolean, String, String](ScriptAsBooleanFalse).futureValue should be (false)
      }
      Given("that the script returns an integer")
      "succeed and return the integer" taggedAs (V260) in {
        client.eval[Long, String, String](
          ScriptAsInteger, keys = Seq("STR")
        ).futureValue should be (Value.size)
      }
      Given("that the script returns a simple string response")
      "succeed and return the message" taggedAs (V260) in {
        client.eval[String, String, String](
          ScriptAsStatus, keys = Seq("STR"), args = Seq("C")
        ).futureValue should be ("OK")
        client.get("STR").futureValue should contain ("C")
      }
      Given("that the script returns a string")
      "succeed and return the string" taggedAs (V260) in {
        client.eval[Option[String], String, String](
          ScriptAsString, keys = Seq("STR")
        ).futureValue should contain ("C")
        client.eval[Option[String], String, String](
          ScriptAsString, keys = Seq(NonExistentKey)
        ).futureValue should be (empty)
      }
      Given("that the script returns a list")
      "succeed and return the list" taggedAs (V260) in {
        client.eval[List[String], String, Long](
          ScriptAsList, keys = Seq("LIST"), args = Seq(0, -1)
        ).futureValue should be (List("A", "B", "C"))
        client.eval[List[String], String, Long](
          ScriptAsList, keys = Seq(NonExistentKey), args = Seq(0, -1)
        ).futureValue should be (empty)
      }
      Given("that the script returns a nested list")
      "succeed and return the nested list" taggedAs (V260) in {
        client.eval[List[Any], String, String](
          ScriptAsNestedList, keys = Seq("1", "2", "3", "4", "5")
        ).futureValue should be (
          List(
            Some("1"),
            List(
              Some("2"),
              List(Some("4"), Some("5")),
              Some("3")
            )
          )
        )
        
        client.eval[List[Any], String, String](
          ScriptAsNestedListWithNils, keys = Seq("1", "2", "3", "4", "5")
        ).futureValue should be (
          List(
            Some("1"),
            List(
              Some("2"),
              List(None, None),
              None
            )
          )
        )
        
        client.eval[List[Any], String, String](ScriptAsEmptyList).futureValue should be (empty)
      }
    }
  }
  
  EvalSHA.toString when {
    "the script is valid but returns an error" should {
      "return an error" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsInteger).!
        a [RedisErrorResponseException] should be thrownBy { 
          client.evalSHA[Long, String, String](sha1, keys = Seq("HASH")).futureValue
        }
      }
    }
    "the script is a valid Lua script" should {
      Given("that the script does not return anything")
      "succeed without returning anything" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsUnit).!
        client.evalSHA[Unit, String, String](
          sha1, keys = Seq("STR"), args = Seq(Value)
        ).futureValue should be (())
        client.get("STR").futureValue should contain (Value)
      }
      Given("that the script returns true")
      "succeed and return true" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsBooleanTrue).!
        client.evalSHA[Boolean, String, String](sha1).futureValue should be (true)
      }
      Given("that the script returns false")
      "succeed and return false" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsBooleanFalse).!
        client.evalSHA[Boolean, String, String](sha1).futureValue should be (false)
      }
      Given("that the script returns an integer")
      "succeed and return the integer" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsInteger).!
        client.evalSHA[Long, String, String](
          sha1, keys = Seq("STR")
        ).futureValue should be (Value.size)
      }
      Given("that the script returns a simple string response")
      "succeed and return the message" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsStatus).!
        client.evalSHA[String, String, String](
          sha1, keys = Seq("STR"), args = Seq("C")
        ).futureValue should be ("OK")
        client.get("STR").futureValue should contain ("C")
      }
      Given("that the script returns a string")
      "succeed and return the string" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsString).!
        client.evalSHA[Option[String], String, String](
          sha1, keys = Seq("STR")
        ).futureValue should contain ("C")
        client.evalSHA[Option[String], String, String](
          sha1, keys = Seq(NonExistentKey)
        ).futureValue should be (empty)
      }
      Given("that the script returns a list")
      "succeed and return the list" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsList).!
        client.evalSHA[List[String], String, Long](
          sha1, keys = Seq("LIST"), args = Seq(0, -1)
        ).futureValue should be (List("A", "B", "C"))
        client.evalSHA[List[String], String, Long](
          sha1, keys = Seq(NonExistentKey), args = Seq(0, -1)
        ).futureValue should be (empty)
      }
      Given("that the script returns a nested list")
      "succeed and return the nested list" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsNestedList).!
        client.evalSHA[List[Any], String, String](
          sha1, keys = Seq("1", "2", "3", "4", "5")
        ).futureValue should be (
          List(
            Some("1"),
            List(
              Some("2"),
              List(Some("4"), Some("5")),
              Some("3")
            )
          )
        )
        val sha2 = client.scriptLoad(ScriptAsNestedListWithNils).!
        client.evalSHA[List[Any], String, String](
          sha2, keys = Seq("1", "2", "3", "4", "5")
        ).futureValue should be (
          List(
            Some("1"),
            List(
              Some("2"),
              List(None, None),
              None
            )
          )
        )
        val sha3 = client.scriptLoad(ScriptAsEmptyList).!
        client.evalSHA[List[Any], String, String](sha3).futureValue should be (empty)
      }
    }
  }
  
  ScriptExists.toString when {
    "some scripts exist" should {
      "return true whenever a script exists and false otherwise" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsStatus).futureValue
        client.scriptExists(
          "asdasd", sha1, "asdhkasdasd"
        ).futureValue should contain theSameElementsAs List(
          "asdasd" -> false, sha1 -> true, "asdhkasdasd" -> false
        )
        client.scriptFlush().futureValue should be (())
      }
    }
  }

  ScriptFlush.toString when {
    "some scripts exist" should {
      "remove all cached scripts" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsUnit).!
        val sha2 = client.scriptLoad(ScriptAsStatus).!
        client.scriptExists(sha1, sha2).futureValue should contain theSameElementsAs List(
          sha1 -> true, sha2 -> true
        )
        client.scriptFlush().futureValue should be (())
        client.scriptExists(sha1, sha2).futureValue should contain theSameElementsAs List(
          sha1 -> false, sha2 -> false
        )
      }
    }
  }
  
  ScriptKill.toString should {
    "always succeed" taggedAs (V260) in {
      client.scriptKill() should be (())
    }
  }
  
  ScriptLoad.toString when {
    "the script is invalid" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.scriptLoad(InvalidScript).!
        }
      }
    }
    "the script is valid" should {
      "store the script in the script cache" taggedAs (V260) in {
        val sha1 = client.scriptLoad(ScriptAsStatus).!
        client.scriptExists(sha1).futureValue should contain theSameElementsAs List(sha1 -> true)
        client.scriptFlush().futureValue should be (())
      }
    }
  }

  override def afterAll() {
    client.flushDB().!
    client.quit().!
  }
}
