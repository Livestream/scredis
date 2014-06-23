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

import scredis.{ Redis, Client }
import scredis.exceptions.{ RedisCommandException, RedisConnectionException }
import scredis.tags._

class ConnectionCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val client = Client()
  private val redis = Redis()
  private val redisWithPassword = Redis("application.conf", "unauthenticated.scredis")
  private val CorrectPassword = "foobar"
    
  import Names._
  import scredis.util.TestUtils._

  Auth when {
    "the server has no password" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.auth("foo").! }
      }
    }
    "the password is invalid" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redisWithPassword.auth("foo").! }
      }
    }
    "the password is correct" should {
      "authenticate to the server" taggedAs (V100) in {
        redisWithPassword.auth(CorrectPassword).!
        redisWithPassword.ping() must be("PONG")
      }
    }
    "re-authenticating with a wrong password" should {
      "return an error and unauthenticate the redis" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redisWithPassword.auth("foo").! }
        redisWithPassword.ping() must be("PONG")
      }
    }
    "re-authenticating with a correct password" should {
      "authenticate back to the server" taggedAs (V100) in {
        redisWithPassword.auth(CorrectPassword).!
        redisWithPassword.ping() must be("PONG")
      }
    }
  }

  Echo should {
    "echo back the message" taggedAs (V100) in {
      redis.echo("Hello World -> 虫àéç蟲") must be("Hello World -> 虫àéç蟲")
    }
  }

  Ping should {
    "receive PONG" taggedAs (V100) in {
      redis.ping() must be("PONG")
    }
  }

  Quit when {
    "quiting" should {
      "close the connection" taggedAs (V100) in {
        client.quit()
        client.isConnected must be(false)
      }
    }
    "issuing a command on a disconnected redis" should {
      "reconnect and succeed" taggedAs (V100) in {
        client.ping() must be("PONG")
        client.isConnected must be(true)
      }
    }
  }

  Select when {
    "database index is negative" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { redis.select(-1).! }
      }
    }
    "database index is too large" should {
      "return an error" taggedAs (V100) in {
        an [RedisCommandException] must be thrownBy { 
          redis.select(Integer.MAX_VALUE).!
        }
      }
    }
    "database index is valid" should {
      "succeed" taggedAs (V100) in {
        redis.select(1).! must be(())
      }
    }
  }

  override def afterAll() {
    client.quit()
    redis.quit()
    redisWithPassword.quit()
  }

}
