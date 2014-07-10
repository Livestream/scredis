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
  
  private val client = Client()
  private val clientToQuit = Client()
  private val clientWithPassword = Client("application.conf", "unauthenticated.scredis")
  private val CorrectPassword = "foobar"
  
  Auth.toString when {
    "the server has no password" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.auth("foo").!
        }
      }
    }
    "the password is invalid" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          clientWithPassword.auth("foo").!
        }
      }
    }
    "the password is correct" should {
      "authenticate to the server" taggedAs (V100) in {
        clientWithPassword.auth(CorrectPassword).futureValue should be (())
        clientWithPassword.ping().futureValue should be ("PONG")
      }
    }
    "re-authenticating with a wrong password" should {
      "return an error and unauthenticate the client" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          clientWithPassword.auth("foo").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          clientWithPassword.ping().!
        }
      }
    }
    "re-authenticating with a correct password" should {
      "authenticate back to the server" taggedAs (V100) in {
        clientWithPassword.auth(CorrectPassword).futureValue should be (())
        clientWithPassword.ping().futureValue should be ("PONG")
      }
    }
  }

  Echo.toString should {
    "echo back the message" taggedAs (V100) in {
      client.echo("Hello World -> 虫àéç蟲").futureValue should be ("Hello World -> 虫àéç蟲")
    }
  }

  Ping.toString should {
    "receive PONG" taggedAs (V100) in {
      client.ping().futureValue should be ("PONG")
    }
  }

  Quit.toString when {
    "quiting" should {
      "close the connection" taggedAs (V100) in {
        clientToQuit.quit().futureValue should be (())
        a [RedisIOException] should be thrownBy {
          clientToQuit.ping().!
        }
      }
    }
  }

  Select.toString when {
    "database index is negative" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.select(-1).!
        }
      }
    }
    "database index is too large" should {
      "return an error" taggedAs (V100) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.select(Integer.MAX_VALUE).!
        }
      }
    }
    "database index is valid" should {
      "succeed" taggedAs (V100) in {
        client.select(1).futureValue should be (())
      }
    }
  }

  override def afterAll() {
    client.quit().!
    clientWithPassword.quit().!
  }
  
}
