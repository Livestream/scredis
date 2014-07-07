package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.ConnectionRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

class ConnectionCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  private val redis = Client()
  private val redisToQuit = Client()
  private val redisWithPassword = Client("application.conf", "unauthenticated.scredis")
  private val CorrectPassword = "foobar"
  
  Auth.name when {
    "the server has no password" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          redis.auth("foo").futureValue
        }
      }
    }
    "the password is invalid" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          redisWithPassword.auth("foo").futureValue
        }
      }
    }
    "the password is correct" should {
      "authenticate to the server" taggedAs (V100) in {
        redisWithPassword.auth(CorrectPassword).futureValue should be (())
        redisWithPassword.ping().futureValue should be("PONG")
      }
    }
    "re-authenticating with a wrong password" should {
      "return an error and unauthenticate the redis" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          redisWithPassword.auth("foo").futureValue
        }
        redisWithPassword.ping().futureValue should be("PONG")
      }
    }
    "re-authenticating with a correct password" should {
      "authenticate back to the server" taggedAs (V100) in {
        redisWithPassword.auth(CorrectPassword).futureValue should be (())
        redisWithPassword.ping().futureValue should be("PONG")
      }
    }
  }

  Echo.name should {
    "echo back the message" taggedAs (V100) in {
      redis.echo("Hello World -> 虫àéç蟲").futureValue should be("Hello World -> 虫àéç蟲")
    }
  }

  Ping.name should {
    "receive PONG" taggedAs (V100) in {
      redis.ping().futureValue should be("PONG")
    }
  }

  Quit.name when {
    "quiting" should {
      "close the connection" taggedAs (V100) in {
        redisToQuit.quit().futureValue should be (())
        a [RedisIOException] should be thrownBy {
          redisToQuit.ping().futureValue
        }
      }
    }
  }

  Select.name when {
    "database index is negative" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { redis.select(-1).futureValue }
      }
    }
    "database index is too large" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          redis.select(Integer.MAX_VALUE).futureValue
        }
      }
    }
    "database index is valid" should {
      "succeed" taggedAs (V100) in {
        redis.select(1).futureValue should be(())
      }
    }
  }

  override def afterAll() {
    redis.quit().futureValue
    redisWithPassword.quit().futureValue
  }
  
}
