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
import scredis.exceptions.RedisCommandException
import scredis.tags._

import java.util.concurrent.Executors

class ScriptingCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
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

  import Names._
  import scredis.util.TestUtils._
  import redis.ec

  override def beforeAll(): Unit = {
    redis.hSet("HASH")("FIELD", "VALUE")
    redis.rPush("LIST", "A")
    redis.rPush("LIST", "B")
    redis.rPush("LIST", "C").!
  }

  "%s %s".format(Script, ScriptLoad) when {
    "the script is invalid" should {
      "return an error" taggedAs (V260) in {
        evaluating { redis.scriptLoad(InvalidScript) } must produce[RedisCommandException]
      }
    }
    "the script is valid" should {
      "store the script in the script cache" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsStatus).!
        redis.scriptExists(sha1)().map(_(0)) must be(true)
        redis.scriptFlush().!
      }
    }
  }

  "%s %s".format(Script, ScriptExists) when {
    "some scripts exist" should {
      "return true whenever a script exists and false otherwise" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsStatus).!
        redis.scriptExists("asdasd", sha1, "asdhkasdasd").map(_.toList) must be(
          List(false, true, false)
        )
        redis.scriptFlush().!
      }
    }
  }

  "%s %s".format(Script, ScriptFlush) when {
    "some scripts exist" should {
      "remove all cached scripts" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsUnit).!
        val sha2 = redis.scriptLoad(ScriptAsStatus).!
        redis.scriptExists(sha1, sha2).map(_.toList) must be(List(true, true))
        redis.scriptFlush().!
        redis.scriptExists(sha1, sha2).map(_.toList) must be(List(false, false))
      }
    }
  }

  Eval when {
    "the script is invalid" should {
      "return an error" taggedAs (V260) in {
        evaluating { redis.eval(InvalidScript)(_.asUnit) } must produce[RedisCommandException]
      }
    }
    "the script is valid but returns an error" should {
      "return an error" taggedAs (V260) in {
        evaluating {
          redis.evalWithKeys(ScriptAsInteger)("HASH")(_.asUnit)
        } must produce[RedisCommandException]
      }
    }
    "the script is a valid Lua script" should {
      Given("that the script does not return anything")
      "succeed without returning anything" taggedAs (V260) in {
        redis.evalWithKeysAndArgs(ScriptAsUnit)("STR")(Value)(_.asUnit)
        redis.get("STR") must be(Some(Value))
      }
      Given("that the script returns true")
      "succeed and return true" taggedAs (V260) in {
        redis.eval(ScriptAsBooleanTrue)(_.asBoolean) must be(true)
      }
      Given("that the script returns false")
      "succeed and return false" taggedAs (V260) in {
        redis.eval(ScriptAsBooleanFalse)(_.asBoolean) must be(false)
      }
      Given("that the script returns an integer")
      "succeed and return the integer" taggedAs (V260) in {
        redis.evalWithKeys(ScriptAsInteger)("STR")(_.asInteger) must be(Value.size)
      }
      Given("that the script returns a status reply")
      "succeed and return the status reply" taggedAs (V260) in {
        redis.evalWithKeysAndArgs(ScriptAsStatus)("STR")("C")(_.asStatus) must be("OK")
        redis.get("STR") must be(Some("C"))
      }
      Given("that the script returns a string")
      "succeed and return the string" taggedAs (V260) in {
        redis.evalWithKeys(ScriptAsString)("STR")(_.asString) must be(Some("C"))
        redis.evalWithKeys(ScriptAsString)(NonExistentKey)(_.asString) must be('empty)
      }
      Given("that the script returns a list")
      "succeed and return the list" taggedAs (V260) in {
        redis.evalWithKeysAndArgs(ScriptAsList)("LIST")(0, -1)(_.asList) must be(List(
          "A", "B", "C"
        ))
        redis.evalWithKeysAndArgs(ScriptAsList)(NonExistentKey)(0, -1)(_.asList) must be('empty)
      }
      Given("that the script returns a nested list")
      "succeed and return the nested list" taggedAs (V260) in {
        redis.evalWithKeys(ScriptAsNestedList)("1", "2", "3", "4", "5")(
          _.asNestedList
        ) must be(List("1", List("2", List("4", "5"), "3")))

        redis.evalWithKeys(ScriptAsNestedListWithNils)("1", "2", "3", "4", "5")(
          _.asNestedList
        ) must be(List("1", List("2", List())))

        redis.eval(ScriptAsEmptyList)(_.asNestedList) must be('empty)
      }
    }
  }
  
  EvalSha when {
    "the script is valid but returns an error" should {
      "return an error" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsInteger).!
        evaluating {
          redis.evalShaWithKeys(sha1)("HASH")(_.asUnit)
        } must produce[RedisCommandException]
      }
    }
    "the script is a valid Lua script" should {
      Given("that the script does not return anything")
      "succeed without returning anything" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsUnit).!
        redis.evalShaWithKeysAndArgs(sha1)("STR")(Value)(_.asUnit)
        redis.get("STR") must be(Some(Value))
      }
      Given("that the script returns true")
      "succeed and return true" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsBooleanTrue).!
        redis.evalSha(sha1)(_.asBoolean) must be(true)
      }
      Given("that the script returns false")
      "succeed and return false" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsBooleanFalse).!
        redis.evalSha(sha1)(_.asBoolean) must be(false)
      }
      Given("that the script returns an integer")
      "succeed and return the integer" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsInteger).!
        redis.evalShaWithKeys(sha1)("STR")(_.asInteger) must be(Value.size)
      }
      Given("that the script returns a status reply")
      "succeed and return the status reply" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsStatus).!
        redis.evalShaWithKeysAndArgs(sha1)("STR")("C")(_.asStatus) must be("OK")
        redis.get("STR") must be(Some("C"))
      }
      Given("that the script returns a string")
      "succeed and return the string" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsString).!
        redis.evalShaWithKeys(sha1)("STR")(_.asString) must be(Some("C"))
        redis.evalShaWithKeys(sha1)(NonExistentKey)(_.asString) must be('empty)
      }
      Given("that the script returns a list")
      "succeed and return the list" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsList).!
        redis.evalShaWithKeysAndArgs(sha1)("LIST")(0, -1)(_.asList) must be(List("A", "B", "C"))
        redis.evalShaWithKeysAndArgs(sha1)(NonExistentKey)(0, -1)(_.asList) must be('empty)
      }
      Given("that the script returns a nested list")
      "succeed and return the nested list" taggedAs (V260) in {
        val sha1 = redis.scriptLoad(ScriptAsNestedList).!
        val sha2 = redis.scriptLoad(ScriptAsNestedListWithNils).!
        val sha3 = redis.scriptLoad(ScriptAsEmptyList).!
        
        redis.evalShaWithKeys(sha1)("1", "2", "3", "4", "5")(
          _.asNestedList
        ) must be(List("1", List("2", List("4", "5"), "3")))

        redis.evalShaWithKeys(sha2)("1", "2", "3", "4", "5")(_.asNestedList) must be(
          List("1", List("2", List()))
        )

        redis.evalSha(sha3)(_.asNestedList) must be('empty)
        
        redis.scriptFlush()
      }
    }
  }

  override def afterAll() {
    redis.flushDb().!
    redis.quit()
  }
}
