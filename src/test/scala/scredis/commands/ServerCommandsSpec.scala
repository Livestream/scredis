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

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll, OneInstancePerTest }
import org.scalatest.matchers.MustMatchers._

import scredis.{ Redis, Client }
import scredis.exceptions._
import scredis.tags._

class ServerCommandsSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val redis = Redis()
  private val client1 = Client(port = 6380, password = Some("foobar"))
  private val client2 = Client(port = 6380, password = Some("foobar"))
  private val client3 = Client(port = 6380, password = Some("foobar"))

  import Names._
  import scredis.util.TestUtils._
  import redis.ec

  BgRewriteAOF should {
    "succeed" taggedAs (V100) in {
      redis.bgRewriteAOF().!
      Thread.sleep(500)
    }
  }

  BgSave when {
    "not already running" should {
      "succeed" taggedAs (V100) in {
        redis.bgSave().!
        Thread.sleep(500)
      }
    }
    "already running" should {
      "return an error" taggedAs (V100) in {
        evaluating {
          redis.bgSave()
          redis.bgSave()
        } must produce[RedisCommandException]
        Thread.sleep(500)
      }
    }
    "%s is already running".format(BgRewriteAOF) should {
      "return an error" taggedAs (V100) in {
        evaluating {
          redis.bgRewriteAOF()
          redis.bgSave()
        } must produce[RedisCommandException]
      }
    }
  }

  "%s %s".format(Names.Client, ClientGetName) when {
    "client has no name" should {
      "return None" taggedAs (V269) in {
        redis.clientGetName() must be('empty)
      }
    }
    "client has a name" should {
      "return client's name" taggedAs (V269) in {
        redis.clientSetName("foo")
        redis.clientGetName() must be(Some("foo"))
      }
    }
  }

  "%s %s".format(Names.Client, ClientSetName) when {
    "name is not empty" should {
      "set the specified name" taggedAs (V269) in {
        redis.clientSetName("bar")
        redis.clientGetName() must be(Some("bar"))
      }
    }
    "name is the empty string" should {
      "unset client's name" taggedAs (V269) in {
        redis.clientSetName("")
        redis.clientGetName() must be('empty)
      }
    }
  }

  "%s %s".format(Names.Client, ClientList) when {
    "async" should {
      "suceed" taggedAs (V240) in {
        redis.clientListRaw() must not be('empty)
        redis.clientList() must not be('empty)
      }
    }
    "3 clients are connected" should {
      "list the 3 clients" taggedAs (V240) in {
        client1.clientListRaw().split("\\n") must have size (3)
        val clients = client1.clientList()
        clients must have size (3)
        clients.foreach(data => {
          data.contains("addr") must be(true)
          data.contains("fd") must be(true)
          data.contains("age") must be(true)
          data.contains("idle") must be(true)
          data.contains("flags") must be(true)
          data.contains("db") must be(true)
          data.contains("sub") must be(true)
          data.contains("psub") must be(true)
          data.contains("multi") must be(true)
          data.contains("qbuf") must be(true)
          data.contains("qbuf-free") must be(true)
          data.contains("obl") must be(true)
          data.contains("oll") must be(true)
          data.contains("omem") must be(true)
          data.contains("events") must be(true)
          data.contains("cmd") must be(true)
        })
      }
    }
    "2 clients are connected" should {
      "list the 2 clients" taggedAs (V240) in {
        client3.quit()
        client1.clientList() must have size (2)
      }
    }
    "no other clients are connected" should {
      "list the calling client" taggedAs (V240) in {
        client2.quit()
        client1.clientList() must have size (1)
      }
    }
  }

  "%s %s".format(Names.Client, ClientKill) when {
    "providing invalid ip and port" should {
      "return an error" taggedAs (V240) in {
        evaluating { redis.clientKillFromIpPort("lol", -1) } must produce[RedisCommandException]
      }
    }
    "killing a non-existing client" should {
      "return an error" taggedAs (V240) in {
        evaluating {
          redis.clientKillFromIpPort("110.44.56.127", 53655)
        } must produce[RedisCommandException]
      }
    }
    "killing a connected client" should {
      "succeed" taggedAs (V240) in {
        client2.ping() must be("PONG")
        val client2Data = client1.clientList().reduceLeft((c1, c2) => {
          if (c1("age").toInt < c2("age").toInt) c1
          else c2
        })
        client1.clientKill(client2Data("addr"))
        client1.clientList() must have size (1)
      }
    }
    "killing self" should {
      "succeed" taggedAs (V240) in {
        val clientData = client1.clientList()()(0)
        client1.clientKill(clientData("addr"))
      }
    }
  }

  "%s %s".format(Config, ConfigGet) when {
    "providing an non-existent key" should {
      "return None" taggedAs (V200) in {
        redis.configGet("thiskeywillneverexist") must be('empty)
      }
    }
    "not providing any pattern" should {
      "use * and return all" taggedAs (V200) in {
        redis.configGet().map(_.get.size) must be > (1)
      }
    }
    "providing a valid key that is empty" should {
      "return a None value" taggedAs (V200) in {
        redis.configGet("requirepass").map(_.get("requirepass")) must be('empty)
      }
    }
    "providing a valid key that is not empty" should {
      "return the key's value" taggedAs (V200) in {
        redis.configGet("port").map(_.get("port").get.toInt) must be(6379)
      }
    }
  }

  "%s %s".format(Config, ConfigSet) when {
    "providing an non-existent key" should {
      "return an error" taggedAs (V200) in {
        evaluating {
          redis.configSet("thiskeywillneverexist", "foo")
        } must produce[RedisCommandException]
      }
    }
    "changing the password" should {
      "succeed" taggedAs (V200) in {
        redis.configSet("requirepass", "guessit")
        evaluating { redis.ping() } must produce[RedisCommandException]
        redis.auth("guessit").!
        redis.configGet("requirepass").map(_.get("requirepass").get) must be("guessit")
        redis.configSet("requirepass", "")
        redis.configGet("requirepass").map(_.get("requirepass")) must be('empty)
        redis.auth("").!
      }
    }
  }

  "%s %s".format(Config, ConfigResetStat) should {
    "reset stats" taggedAs (V200) in {
      redis.info()().map(_("total_commands_processed").toInt) must be > (1)
      redis.configResetStat().!
      redis.info()().map(_("total_commands_processed").toInt) must be(1)
    }
  }

  DbSize should {
    "return the size of the current database" taggedAs (V100) in {
      redis.select(0).!
      redis.flushDb().!
      redis.set("SOMEKEY", "SOMEVALUE").!
      redis.dbSize() must be(1)
      redis.select(1).!
      redis.dbSize() must be(0)
      redis.set("SOMEKEY", "SOMEVALUE").!
      redis.set("SOMEKEY2", "SOMEVALUE2").!
      redis.dbSize() must be(2)
    }
  }

  FlushDb should {
    "flush the current database only" taggedAs (V100) in {
      redis.select(0).!
      redis.flushDb().!
      redis.dbSize() must be(0)
      redis.select(1).!
      redis.dbSize() must be(2)
    }
  }

  FlushAll should {
    "flush all databases" taggedAs (V100) in {
      redis.select(0).!
      redis.set("SOMEKEY", "SOMEVALUE").!
      redis.flushAll().!
      redis.dbSize() must be(0)
      redis.select(1).!
      redis.dbSize() must be(0)
    }
  }

  Info when {
    "not provided with any section" should {
      "return the default section" taggedAs (V100) in {
        val str = redis.infoRaw().!
        str.contains("# Server") must be(true)
        str.contains("# Clients") must be(true)
        str.contains("# Memory") must be(true)
        str.contains("# Persistence") must be(true)
        str.contains("# Stats") must be(true)
        str.contains("# Replication") must be(true)
        str.contains("# CPU") must be(true)
        str.contains("# Keyspace") must be(true)

        val map = redis.info().!
        map.contains("redis_version") must be(true)
        map.contains("connected_clients") must be(true)
        map.contains("used_memory") must be(true)
        map.contains("loading") must be(true)
        map.contains("total_connections_received") must be(true)
        map.contains("role") must be(true)
        map.contains("used_cpu_sys") must be(true)
      }
    }
    "provided with a non-existent section" should {
      "return an empty string/map" taggedAs (V260) in {
        redis.infoBySectionRaw("YOU BET") must be('empty)
        redis.infoBySection("YOU BET") must be('empty)
      }
    }
    "provided with a valid section" should {
      "only return the selected section" taggedAs (V260) in {
        val str = redis.infoBySectionRaw("server").!
        str.contains("redis_version") must be(true)
        str.contains("connected_clients") must be(false)
        val map = redis.infoBySection("server").!
        map.contains("redis_version") must be(true)
        map.contains("connected_clients") must be(false)
      }
    }
  }

  LastSave should {
    "return the UNIX timestamp of the last database save" taggedAs (V100) in {
      redis.lastSave().!
    }
  }

  Save should {
    "save the database" taggedAs (V100) in {
      redis.save().!
    }
  }

  Time should {
    "return the current server UNIX timestamp with microseconds" taggedAs (V260) in {
      redis.time().!
    }
  }

  override def afterAll(): Unit = {
    redis.flushAll().!
    redis.quit()
    client1.flushAll()
    client1.quit()
    client2.quit()
    client3.quit()
  }

}