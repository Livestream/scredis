package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.KeyRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class KeyCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with EitherValues
  with ScalaFutures {
  
  private val client = Client()
  private val client2 = Client("application.conf", "authenticated.scredis")
  private val SomeValue = "HelloWorld!虫àéç蟲"

  private var dumpedValue: Array[Byte] = _

  private def setUpSortData(): Unit = {
    client.rPush("LIST", "1")
    client.rPush("LIST", "3")
    client.rPush("LIST", "5")
    client.rPush("LIST", "2")
    client.rPush("LIST", "4")
    client.rPush("LIST-ALPHA", "A")
    client.rPush("LIST-ALPHA", "C")
    client.rPush("LIST-ALPHA", "E")
    client.rPush("LIST-ALPHA", "B")
    client.rPush("LIST-ALPHA", "D")

    client.sAdd("SET", "1")
    client.sAdd("SET", "3")
    client.sAdd("SET", "5")
    client.sAdd("SET", "2")
    client.sAdd("SET", "4")
    client.sAdd("SET-ALPHA", "A")
    client.sAdd("SET-ALPHA", "C")
    client.sAdd("SET-ALPHA", "E")
    client.sAdd("SET-ALPHA", "B")
    client.sAdd("SET-ALPHA", "D")

    client.zAdd("ZSET", "1", Score.Value(5))
    client.zAdd("ZSET", "3", Score.Value(3))
    client.zAdd("ZSET", "5", Score.Value(1))
    client.zAdd("ZSET", "2", Score.Value(4))
    client.zAdd("ZSET", "4", Score.Value(2))
    client.zAdd("ZSET-ALPHA", "A", Score.Value(5))
    client.zAdd("ZSET-ALPHA", "C", Score.Value(3))
    client.zAdd("ZSET-ALPHA", "E", Score.Value(1))
    client.zAdd("ZSET-ALPHA", "B", Score.Value(4))
    client.zAdd("ZSET-ALPHA", "D", Score.Value(2))

    client.set("WEIGHT-1", "1")
    client.set("WEIGHT-2", "2")
    client.set("WEIGHT-3", "3")
    client.set("WEIGHT-4", "4")
    client.set("WEIGHT-5", "5")

    client.set("WEIGHT-A", "1")
    client.set("WEIGHT-B", "2")
    client.set("WEIGHT-C", "3")
    client.set("WEIGHT-D", "4")
    client.set("WEIGHT-E", "5")

    client.set("VALUE-1", "V1")
    client.set("VALUE-2", "V2")
    client.set("VALUE-3", "V3")
    client.set("VALUE-4", "V4")
    client.set("VALUE-5", "V5")

    client.set("VALUE-A", "VA")
    client.set("VALUE-B", "VB")
    client.set("VALUE-C", "VC")
    client.set("VALUE-D", "VD")
    client.set("VALUE-E", "VE").futureValue
  }

  Del.name when {
    "deleting a single key that does not exist" should {
      "return 0" taggedAs (V100) in {
        client.del("I-DONT-EXIST").futureValue should be (0)
      }
    }
    "deleting a single key that exists" should {
      "delete the key" taggedAs (V100) in {
        client.set("I-EXIST", "YES")
        client.del("I-EXIST").futureValue should be (1)
        client.del("I-EXIST").futureValue should be (0)
      }
    }
    "deleting multiple keys that partially exist" should {
      "delete the existing keys" taggedAs (V100) in {
        client.set("I-EXIST", "YES")
        client.set("I-EXIST-2", "YES")
        client.del("I-EXIST", "I-EXIST-2", "I-EXIST-3").futureValue should be (2)
        client.del("I-EXIST", "I-EXIST-2", "I-EXIST-3").futureValue should be (0)
      }
    }
  }

  Dump.name when {
    "the key does not exist" should {
      "return None" taggedAs (V260) in {
        client.dump("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key exists" should {
      "return the serialized value for that key" taggedAs (V260) in {
        client.set("TO-DUMP", SomeValue)
        val dump = client.dump[Array[Byte]]("TO-DUMP")(scredis.serialization.BytesReader).!
        dump should be (defined)
        dumpedValue = dump.get
      }
    }
  }

  Exists.name when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.exists("NONEXISTENTKEY").futureValue should be (false)
      }
    }
    "the key exists" should {
      "return true" taggedAs (V100) in {
        client.exists("TO-DUMP").futureValue should be (true)
      }
    }
  }

  Expire.name when {
    "the key does not exist" should {
      "return false" taggedAs (V100) in {
        client.expire("NONEXISTENTKEY", 1).futureValue should be (false)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs (V100) in {
        client.set("TO-EXPIRE", "HEY")
        client.expire("TO-EXPIRE", 1).futureValue should be (true)
        client.get("TO-EXPIRE").futureValue should be (defined)
        Thread.sleep(1050)
        client.get("TO-EXPIRE").futureValue should be (empty)
      }
    }
  }

  ExpireAt.name when {
    "the key does not exist" should {
      "return false" taggedAs (V120) in {
        val unixTimestamp = (System.currentTimeMillis / 1000)
        client.expireAt("NONEXISTENTKEY", unixTimestamp).futureValue should be (false)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs (V120) in {
        val unixTimestamp = (System.currentTimeMillis / 1000) + 1
        client.set("TO-EXPIRE", "HEY")
        client.expireAt("TO-EXPIRE", unixTimestamp).futureValue should be (true)
        client.get("TO-EXPIRE").futureValue should be (defined)
        Thread.sleep(1050)
        client.get("TO-EXPIRE").futureValue should be (empty)
      }
    }
  }

  Keys.name when {
    "the database is empty" should {
      "return None" taggedAs (V100) in {
        client.flushAll()
        client.keys("*").futureValue should be (empty)
      }
    }
    "the database contains three keys" should {
      "return the keys accordingly" taggedAs (V100) in {
        client.set("a", "a")
        client.set("ab", "b")
        client.set("abc", "c")
        client.keys("*").futureValue should have size (3)
        client.keys("a*").futureValue should have size (3)
        client.keys("ab*").futureValue should have size (2)
        client.keys("abc*").futureValue should have size (1)
        client.keys("ab").futureValue should have size (1)
      }
    }
  }

  Migrate.name when {
    "the key does not exist" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.migrate("NONEXISTENTKEY", "127.0.0.1").futureValue
        }
      }
    }
    "migrating a key to the same instance" should {
      "return an error" taggedAs (V260) in {
        client.set("TO-MIGRATE", SomeValue).futureValue
        a [RedisErrorResponseException] should be thrownBy { 
          client.migrate("TO-MIGRATE", "127.0.0.1", timeout = 500 milliseconds).futureValue
        }
      }
    }
    "migrating a key to a non-existing instance" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.migrate("TO-MIGRATE", "127.0.0.1", 6378, timeout = 500 milliseconds).futureValue
        }
      }
    }
    "migrating a key to a valid instance" should {
      "succeed" taggedAs (V260) in {
        // Remove password on 6380
        client2.configSet("requirepass", "").futureValue
        client2.auth("").futureValue
        client.migrate("TO-MIGRATE", "127.0.0.1", 6380, timeout = 500 milliseconds).futureValue
        client.exists("TO-MIGRATE").futureValue should be (false)
        client2.get("TO-MIGRATE").futureValue should contain (SomeValue)
        // Set password back on 6380
        client2.configSet("requirepass", "foobar").futureValue
        client2.auth("foobar").futureValue
      }
    }
    "migrating a key to a valid instance in another database" should {
      "succeed" taggedAs (V260) in {
        client2.migrate(
          "TO-MIGRATE", "127.0.0.1", 6379, database = 1, timeout = 500 milliseconds
        ).futureValue
        client2.exists("TO-MIGRATE").futureValue should be (false)
        client.select(1).futureValue
        client.get("TO-MIGRATE").futureValue should be (SomeValue)
        client.flushDB().futureValue should be (())
        client.select(0).futureValue should be (())
      }
    }
    "migrating a key to a valid instance with COPY option enabled" should {
      "succeed and keep the original key" is (pending)
    }
    "migrating a key to a valid instance with REPLACE option enabled" should {
      "succeed and replace the target key" is (pending)
    }
  }

  Move.name when {
    "moving a key that does not exist" should {
      "return false" taggedAs (V100) in {
        client.move("NONEXISTENTKEY", 1).futureValue should be (false)
      }
    }
    "moving a key from database 0 to 1" should {
      Given("that the key does not exist in database 1 yet")
      "succeed" taggedAs (V100) in {
        client.set("TO-MOVE", SomeValue).futureValue
        client.move("TO-MOVE", 1).futureValue should be (true)
        client.exists("TO-MOVE").futureValue should be (false)
        client.select(1).futureValue
        client.get("TO-MOVE").futureValue should contain (SomeValue)
      }
      Given("that the key already exists in database 1")
      "fail" taggedAs (V100) in {
        client.select(0).futureValue
        client.set("TO-MOVE", SomeValue).futureValue
        client.move("TO-MOVE", 1).futureValue should be (false)
        client.exists("TO-MOVE").futureValue should be (true)
        client.flushAll().futureValue
      }
    }
  }
  
  ObjectRefCount.name when {
    "the object does not exist" should {
      "return None" taggedAs (V223) in {
        client.objectRefCount("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its ref count" taggedAs (V223) in {
        client.set("OBJECT-REF-COUNT", SomeValue)
        client.objectRefCount("OBJECT-REF-COUNT").futureValue should be (defined)
      }
    }
  }
  
  ObjectEncoding.name when {
    "the object does not exist" should {
      "return None" taggedAs (V223) in {
        client.objectEncoding("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its encoding" taggedAs (V223) in {
        client.set("OBJECT-ENCODING", SomeValue)
        client.objectEncoding("OBJECT-ENCODING").futureValue should be (defined)
      }
    }
  }
  
  ObjectIdleTime.name when {
    "the object does not exist" should {
      "return None" taggedAs (V223) in {
        client.objectIdleTime("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the object exists" should {
      "return its idle time" taggedAs (V223) in {
        client.set("OBJECT-IDLE-TIME", SomeValue)
        client.objectIdleTime("OBJECT-IDLE-TIME").futureValue should be (defined)
      }
    }
  }

  Persist.name when {
    "persisting a non-existent key" should {
      "return false" taggedAs (V220) in {
        client.persist("NONEXISTENTKEY").futureValue should be (false)
      }
    }
    "persisting a key that has no ttl" should {
      "return false" taggedAs (V220) in {
        client.set("TO-PERSIST", SomeValue)
        client.persist("TO-PERSIST").futureValue should be (false)
      }
    }
    "persisting a key that has a ttl" should {
      "return true and the key should not expire" taggedAs (V220) in {
        client.pExpire("TO-PERSIST", 500)
        client.persist("TO-PERSIST").futureValue should be (true)
        Thread.sleep(550)
        client.exists("TO-PERSIST").futureValue should be (true)
      }
    }
  }
  
  PExpire.name when {
    "the key does not exist" should {
      "return false" taggedAs (V260) in {
        client.pExpire("NONEXISTENTKEY", 500).futureValue should be (false)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs (V260) in {
        client.set("TO-EXPIRE", "HEY")
        client.pExpire("TO-EXPIRE", 500).futureValue should be (true)
        client.get("TO-EXPIRE").futureValue should be (defined)
        Thread.sleep(550)
        client.get("TO-EXPIRE").futureValue should be (empty)
      }
    }
  }

  PExpireAt.name when {
    "the key does not exist" should {
      "return false" taggedAs (V260) in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        client.pExpireAt("NONEXISTENTKEY", unixTimestampMillis).futureValue should be (false)
      }
    }
    "the key exists" should {
      "return true and the target key should expire after the ttl" taggedAs (V260) in {
        val unixTimestampMillis = System.currentTimeMillis + 500
        client.set("TO-EXPIRE", "HEY")
        client.pExpireAt("TO-EXPIRE", unixTimestampMillis).futureValue should be (true)
        client.get("TO-EXPIRE").futureValue should be (defined)
        Thread.sleep(550)
        client.get("TO-EXPIRE").futureValue should be (empty)
      }
    }
  }
  
  PTTL.name when {
    "key does not exist" should {
      "return Left(false)" taggedAs (V260) in {
        client.pTtl("NONEXISTENTKEY").futureValue should be (Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return Left(true)" taggedAs (V280) in {
        client.set("TO-TTL", SomeValue)
        client.pTtl("TO-TTL").futureValue should be (Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return None" taggedAs (V260) in {
        client.pExpire("TO-TTL", 500)
        client.pTtl("TO-TTL").futureValue.right.value should be <= (500L)
        client.del("TO-TTL")
      }
    }
  }

  RandomKey.name when {
    "the database is empty" should {
      "return None" taggedAs (V100) in {
        client.flushDB()
        client.randomKey().futureValue should be (empty)
      }
    }
    "the database has some keys" should {
      "return None" taggedAs (V100) in {
        client.set("key1", "value1")
        client.set("key2", "value2")
        client.set("key3", "value3")
        client.randomKey().futureValue should contain oneOf ("key1", "key2", "key3")
      }
    }
  }

  Rename.name when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.rename("sourceKey", "destKey").futureValue
        }
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          client.rename("sourceKey", "sourceKey").futureValue
        }
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs (V100) in {
        client.rename("sourceKey", "destKey")
        client.exists("sourceKey").futureValue should be (false)
        client.get("destKey").futureValue should contain (SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "succeed and overwrite destKey" taggedAs (V100) in {
        client.set("sourceKey", "OTHERVALUE")
        client.rename("sourceKey", "destKey")
        client.exists("sourceKey").futureValue should be (false)
        client.get("destKey").futureValue should contain ("OTHERVALUE")
      }
    }
  }

  RenameNX.name when {
    "the key does not exist" should {
      "return an error" taggedAs (V100) in {
        client.del("sourceKey", "destKey")
        a [RedisErrorResponseException] should be thrownBy { 
          client.renameNX("sourceKey", "destKey").futureValue
        }
      }
    }
    "the source key exists but destination key is identical to source key" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", SomeValue)
        a [RedisErrorResponseException] should be thrownBy { 
          client.renameNX("sourceKey", "sourceKey").futureValue
        }
      }
    }
    "the source key exists and destination key is different" should {
      "succeed" taggedAs (V100) in {
        client.renameNX("sourceKey", "destKey").futureValue should be (true)
        client.exists("sourceKey").futureValue should be (false)
        client.get("destKey").futureValue should contain (SomeValue)
      }
    }
    "the source key exists and destination key is different but already exists" should {
      "return an error" taggedAs (V100) in {
        client.set("sourceKey", "OTHERVALUE")
        client.renameNX("sourceKey", "destKey").futureValue should be (false)
      }
    }
  }
  
  Restore.name when {
    "the destination key already exists" should {
      "return an error" taggedAs (V260) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.restore("TO-DUMP", dumpedValue).futureValue
        }
      }
    }
    "the destination key does not exist" should {
      "restore the dumped value" taggedAs (V260) in {
        client.restore("RESTORED", dumpedValue)
        val dumped = client.get("TO-DUMP").!.get
        client.get("RESTORED").futureValue should contain (dumped)
        client.del("RESTORED")
      }
    }
    "applying a ttl" should {
      "restore the dumped value which should expire after the ttl" taggedAs (V260) in {
        client.restore("RESTORED", dumpedValue, Some(500 milliseconds))
        val dumped = client.get("TO-DUMP").!.get
        client.get("RESTORED").futureValue should contain (dumped)
        Thread.sleep(600)
        client.get("RESTORED").futureValue should be (empty)
      }
    }
  }
  
  Scan.name when {
    "the database is empty" should {
      "return an empty set" taggedAs (V280) in {
        client.flushDB().futureValue should be (())
        val (next, set) = client.scan(0).!
        next should be (0)
        set should be (empty)
      }
    }
    "the database contains 5 keys" should {
      "return all keys" taggedAs (V280) in {
        for (i <- 1 to 5) {
          client.set("key" + i, SomeValue)
        }
        val (next, set) = client.scan(0).!
        next should be (0)
        set should contain theSameElementsAs List(
          "key1", "key2", "key3", "key4", "key5"
        )
        for (i <- 1 to 10) {
          client.set("foo" + i, SomeValue)
        }
      }
    }
    "the database contains 15 keys" should {
      val full = ListBuffer[String]()
      for (i <- 1 to 5) {
        full += ("key" + i)
      }
      for (i <- 1 to 10) {
        full += ("foo" + i)
      }
      val fullList = full.toList
      
      Given("that no pattern is set")
      "return all keys" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(cursor).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList
      }
      Given("that a pattern is set")
      "return all matching keys" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(cursor, matchOpt = Some("foo*")).!
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
      Given("that a pattern is set and count is set to 100")
      "return all matching keys in one iteration" taggedAs (V280) in {
        val keys = ListBuffer[String]()
        var cursor = 0L
        do {
          val (next, set) = client.scan(
            cursor, matchOpt = Some("foo*"), countOpt = Some(100)
          ).!
          set.size should be (10)
          keys ++= set
          cursor = next
        }
        while (cursor > 0)
        keys.toList should contain theSameElementsAs fullList.filter(_.startsWith("foo"))
      }
    }
  }
  
  Sort.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        setUpSortData()
        client.sort("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key contains a list" should {
      Given("that the list contains numbers")
      "sort the list accordingly" taggedAs (V100) in {
        client.sort(
          "LIST", byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("3"), Some("5"), Some("2"), Some("4")
        )
        
        client.sort("LIST").futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "LIST", desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        )
        
        client.sort[String](
          "LIST", desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("4"), Some("3")
        )
        
        client.sort(
          "LIST", byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "LIST", get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        )
        
        client.sort[String](
          "LIST", get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"),
          Some("1"),
          Some("V2"),
          Some("2"),
          Some("V3"),
          Some("3"),
          Some("V4"),
          Some("4"),
          Some("V5"),
          Some("5")
        )
      }
      Given("that the list contains strings")
      "lexicographically sort the list" taggedAs (V100) in {
        client.sort[String](
          "LIST-ALPHA", alpha = true, byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("C"), Some("E"), Some("B"), Some("D")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true, desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true, desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("D"), Some("C")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true, byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true, get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        )
        
        client.sort[String](
          "LIST-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"),
          Some("1"),
          Some("VB"),
          Some("2"),
          Some("VC"),
          Some("3"),
          Some("VD"),
          Some("4"),
          Some("VE"),
          Some("5")
        )
      }
    }
    "the key contains a set" should {
      Given("that the set contains numbers")
      "sort the set accordingly" taggedAs (V100) in {
        client.sort(
          "SET", byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort("SET").futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "SET", desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        )
        
        client.sort[String](
          "SET", desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("4"), Some("3")
        )
        
        client.sort[String](
          "SET", byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "SET", get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        )
        
        client.sort[String](
          "SET", get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"),
          Some("1"),
          Some("V2"),
          Some("2"),
          Some("V3"),
          Some("3"),
          Some("V4"),
          Some("4"),
          Some("V5"),
          Some("5")
        )
      }
      Given("that the set contains strings")
      "lexicographically sort the set" taggedAs (V100) in {
        client.sort[String](
          "SET-ALPHA", alpha = true, byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true, desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true, desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("D"), Some("C")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true, byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true, get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        )
        
        client.sort[String](
          "SET-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"),
          Some("1"),
          Some("VB"),
          Some("2"),
          Some("VC"),
          Some("3"),
          Some("VD"),
          Some("4"),
          Some("VE"),
          Some("5")
        )
      }
    }
    "the key contains a sorted set" should {
      Given("that the sorted set contains numbers")
      "sort the sorted set accordingly" taggedAs (V100) in {
        client.sort(
          "ZSET", byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        )
        
        client.sort("ZSET").futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "ZSET", desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("5"), Some("4"), Some("3"), Some("2"), Some("1")
        )
        
        client.sort[String](
          "ZSET", desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("4"), Some("3")
        )
        
        client.sort[String](
          "ZSET", byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("1"), Some("2"), Some("3"), Some("4"), Some("5")
        )
        
        client.sort[String](
          "ZSET", get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"), Some("V2"), Some("V3"), Some("V4"), Some("V5")
        )
        
        client.sort[String](
          "ZSET", get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("V1"),
          Some("1"),
          Some("V2"),
          Some("2"),
          Some("V3"),
          Some("3"),
          Some("V4"),
          Some("4"), 
          Some("V5"),
          Some("5")
        )
      }
      Given("that the sorted set contains strings")
      "lexicographically sort the sorted set" taggedAs (V100) in {
        client.sort[String](
          "ZSET-ALPHA", alpha = true, byOpt = Some("NOSORT")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true, desc = true
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("E"), Some("D"), Some("C"), Some("B"), Some("A")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true, desc = true, limitOpt = Some((1, 2))
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("D"), Some("C")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true, byOpt = Some("WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("A"), Some("B"), Some("C"), Some("D"), Some("E")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true, get = List("VALUE-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"), Some("VB"), Some("VC"), Some("VD"), Some("VE")
        )
        
        client.sort[String](
          "ZSET-ALPHA", alpha = true, get = List("VALUE-*", "WEIGHT-*")
        ).futureValue should contain theSameElementsInOrderAs List(
          Some("VA"),
          Some("1"),
          Some("VB"),
          Some("2"), 
          Some("VC"),
          Some("3"),
          Some("VD"),
          Some("4"),
          Some("VE"),
          Some("5")
        )
      }
    }
  }
  
  TTL.name when {
    "key does not exist" should {
      "return Left(false)" taggedAs (V100) in {
        client.ttl("NONEXISTENTKEY").futureValue should be (Left(false))
      }
    }
    "key exists but has no ttl" should {
      "return Left(true)" taggedAs (V280) in {
        client.set("TO-TTL", SomeValue)
        client.ttl("TO-TTL").futureValue should be (Left(true))
      }
    }
    "key exists and has a ttl" should {
      "return Right(ttl)" taggedAs (V100) in {
        client.expire("TO-TTL", 1)
        client.ttl("TO-TTL").futureValue.right.value should be <= (1)
        client.del("TO-TTL")
      }
    }
  }

  scredis.protocol.requests.KeyRequests.Type.name when {
    "the key does not exist" should {
      "return None" taggedAs (V100) in {
        client.`type`("NONEXISTENTKEY").futureValue should be (empty)
      }
    }
    "the key is a string" should {
      "return string" taggedAs (V100) in {
        client.set("STRING", "HELLO")
        client.`type`("STRING").futureValue should contain (scredis.Type.String)
        client.del("STRING")
      }
    }
    "the key is a hash" should {
      "return hash" taggedAs (V100) in {
        client.hSet("HASH", "FIELD", "VALUE")
        client.`type`("HASH").futureValue should contain (scredis.Type.Hash)
        client.del("HASH")
      }
    }
    "the key is a list" should {
      "return list" taggedAs (V100) in {
        client.rPush("LIST", "HELLO")
        client.`type`("LIST").futureValue should contain (scredis.Type.List)
        client.del("LIST")
      }
    }
    "the key is a set" should {
      "return set" taggedAs (V100) in {
        client.sAdd("SET", "HELLO")
        client.`type`("SET").futureValue should contain (scredis.Type.Set)
        client.del("SET")
      }
    }
    "the key is a sorted set" should {
      "return zset" taggedAs (V100) in {
        client.zAdd("SORTED-SET", "HELLO", Score.Value(0))
        client.`type`("SORTED-SET").futureValue should contain (scredis.Type.SortedSet)
        client.del("SORTED-SET")
      }
    }
  }

  override def afterAll() {
    client.flushAll().!
    client2.flushAll().!
    client.quit().!
    client2.quit().!
  }

}
