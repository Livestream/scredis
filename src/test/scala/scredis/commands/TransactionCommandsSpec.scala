package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.TransactionRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.util.{ Success, Failure }
import scala.concurrent.Future
import scala.concurrent.duration._

class TransactionCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val client = Client()
  private val SomeValue = "HelloWorld!虫àéç蟲"
  
  override def beforeAll(): Unit = {
    client.lPush("LIST", "A").!
  }
  
  Watch.toString when {
    "the key does not exist" should {
      "succeed" taggedAs (V220) in {
        client.watch("NONEXISTENTKEY").futureValue should be (())
      }
    }
    "the key exists" should {
      "succeed" taggedAs (V220) in {
        client.set("STR", SomeValue)
        client.watch("STR").futureValue should be (())
      }
    }
    "watching multiple keys" should {
      "succeed" taggedAs (V220) in {
        client.set("STR2", "HelloWorld!")
        client.watch("STR", "STR2", "LOL").futureValue should be (())
        client.del("STR", "STR2")
      }
    }
  }
  
  Unwatch.toString should {
    "always succeed" taggedAs (V220) in {
      client.unwatch().futureValue should be (())
    }
  }
  
  "inTransaction" when {
    "all commands are valid" should {
      "succeed" taggedAs (V120) in {
        client.set("STR", SomeValue)
        var f1: Future[Boolean] = null
        var f2: Future[Long] = null
        var f3: Future[Option[String]] = null
        client.inTransaction { b =>
          f1 = b.set("STR2", "HelloWorld!")
          f2 = b.sAdd("SET", "A")
          f3 = b.get("STR")
        }.futureValue should contain theSameElementsInOrderAs List(
          Success(true),
          Success(1),
          Success(Some(SomeValue))
        )
        f1.futureValue should be (true)
        f2.futureValue should be (1)
        f3.futureValue should contain (SomeValue)
      }
    }
    "some commands are invalid after EXEC" should {
      "process the valid ones" taggedAs (V120) in {
        var f1: Future[Boolean] = null
        var f2: Future[Long] = null
        var f3: Future[Option[String]] = null
        val results = client.inTransaction { b =>
          f1 = b.set("STR2", "HelloWorld!")
          f2 = b.sAdd("LIST", "A")
          f3 = b.get("STR")
        }.futureValue
        f1.futureValue should be (true)
        a [RedisErrorResponseException] should be thrownBy {
          f2.!
        }
        f3.futureValue should contain (SomeValue)
      }
    }
  }
  
  "withTransaction" when {
    "all commands are valid" should {
      "succeed" taggedAs (V120) in {
        client.set("STR", SomeValue)
        var f1: Future[Boolean] = null
        var f2: Future[Long] = null
        var f3: Future[Option[String]] = null
        client.withTransaction { b =>
          f1 = b.set("STR2", "HelloWorld!")
          f2 = b.sAdd("SET", "B")
          f3 = b.get("STR")
          f3
        }.futureValue should contain (SomeValue)
        f1.futureValue should be (true)
        f2.futureValue should be (1)
        f3.futureValue should contain (SomeValue)
      }
    }
    "some commands are invalid after EXEC" should {
      "process the valid ones" taggedAs (V120) in {
        var f1: Future[Boolean] = null
        var f2: Future[Long] = null
        var f3: Future[Option[String]] = null
        val results = client.withTransaction { b =>
          f1 = b.set("STR2", "HelloWorld!")
          f2 = b.sAdd("LIST", "A")
          f3 = b.get("STR")
          f3
        }.futureValue should contain (SomeValue)
        f1.futureValue should be (true)
        a [RedisErrorResponseException] should be thrownBy {
          f2.!
        }
        f3.futureValue should contain (SomeValue)
      }
    }
  }

  override def afterAll(): Unit = {
    client.flushDB().!
    client.quit().!
  }

}
