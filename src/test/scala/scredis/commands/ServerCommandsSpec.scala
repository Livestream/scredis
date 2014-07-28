package scredis.commands

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.protocol.requests.ServerRequests._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

import scala.concurrent.duration._

class ServerCommandsSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with Inside
  with ScalaFutures {
  
  private val client = Client()
  private val client1 = Client(port = 6380, passwordOpt = Some("foobar"))
  private val client2 = Client(port = 6380, passwordOpt = Some("foobar"))
  private val client3 = Client(port = 6380, passwordOpt = Some("foobar"))
  
  BGRewriteAOF.toString should {
    "succeed" taggedAs (V100) in {
      client.bgRewriteAOF().futureValue should be (())
      Thread.sleep(1000)
    }
  }

  BGSave.toString when {
    "not already running" should {
      "succeed" taggedAs (V100) in {
        client.bgSave().futureValue should be (())
      }
    }
  }

  ClientGetName.toString when {
    "client has no name" should {
      "return None" taggedAs (V269) in {
        client.clientGetName().futureValue should be (empty)
      }
    }
    "client has a name" should {
      "return client's name" taggedAs (V269) in {
        client.clientSetName("foo")
        client.clientGetName().futureValue should contain ("foo")
      }
    }
  }
  
  ClientKill.toString when {
    "providing invalid ip and port" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.clientKill("lol", -1).!
        }
      }
    }
    "killing a non-existing client" should {
      "return an error" taggedAs (V240) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.clientKill("110.44.56.127", 53655).!
        }
      }
    }
    "killing connected clients" should {
      "succeed" taggedAs (V240) in {
        client1.ping().futureValue should be ("PONG")
        // Cannot kill self
        val clients = client1.clientList().futureValue.filter { data =>
          data("cmd").toLowerCase != "client"
        }
        clients should have size (2)
        clients.reverse.foreach { data =>
          val split = data("addr").split(":")
          client1.clientKill(split(0), split(1).toInt).futureValue should be (())
        }
      }
    }
  }
  
  s"${ClientKill.toString}-2.8.12" when {
    "killing by addresses" should {
      "succeed" taggedAs (V2812) in {
        client1.clientSetName("client1").futureValue should be (())
        client2.clientSetName("client2").futureValue should be (())
        client3.clientSetName("client3").futureValue should be (())
        val clients = client1.clientList().futureValue
        val client2Addr = clients.filter { map =>
          map("name") == "client2"
        }.head("addr")
        val (ip2, port2) = {
          val split = client2Addr.split(":")
          (split(0), split(1).toInt)
        }
        client1.clientKillWithFilters(addrOpt = Some((ip2, port2))).futureValue should be (1)
      }
    }
    "killing by ids" should {
      "succeed" taggedAs (V2812) in {
        client1.clientSetName("client1").futureValue should be (())
        client2.clientSetName("client2").futureValue should be (())
        client3.clientSetName("client3").futureValue should be (())
        val clients = client1.clientList().futureValue
        val client3Id = clients.filter { map =>
          map("name") == "client3"
        }.head("id").toInt
        client1.clientKillWithFilters(idOpt = Some(client3Id)).futureValue should be (1)
      }
    }
    "killing by type" should {
      Given("that skipMe is true")
      "kill all clients except self" taggedAs (V2812) in {
        client1.clientSetName("client1").futureValue should be (())
        client2.clientSetName("client2").futureValue should be (())
        client3.clientSetName("client3").futureValue should be (())
        client1.clientKillWithFilters(
          typeOpt = Some(ClientType.Normal), skipMe = true
        ).futureValue should be (2)
      }
      Given("that skipMe is false")
      "kill all clients including self" taggedAs (V2812) in {
        client1.clientSetName("client1").futureValue should be (())
        client2.clientSetName("client2").futureValue should be (())
        client3.clientSetName("client3").futureValue should be (())
        client1.clientKillWithFilters(
          typeOpt = Some(ClientType.Normal), skipMe = false
        ).futureValue should be (3)
      }
    }
  }

  ClientList.toString when {
    "3 clients are connected" should {
      "list the 3 clients" taggedAs (V240) in {
        client2.clientSetName("client2").futureValue should be (())
        client3.clientSetName("client3").futureValue should be (())
        client1.clientList().futureValue should have size (3)
      }
    }
    "2 clients are connected" should {
      "list the 2 clients" taggedAs (V240) in {
        client3.quit().futureValue should be (())
        client1.clientList().futureValue should have size (2)
      }
    }
    "no other clients are connected" should {
      "list the calling client" taggedAs (V240) in {
        client2.quit().futureValue should be (())
        client1.clientList().futureValue should have size (1)
      }
    }
  }
  
  /* FIXME: add when redis 3.0.0 is out
  ClientPause.toString should {
    "succeed" taggedAs (V2950) in {
      client.clientPause(500).futureValue should be (())
    }
  }*/
  
  ClientSetName.toString when {
    "name is not empty" should {
      "set the specified name" taggedAs (V269) in {
        client.clientSetName("bar")
        client.clientGetName().futureValue should contain ("bar")
      }
    }
    "name is the empty string" should {
      "unset client's name" taggedAs (V269) in {
        client.clientSetName("")
        client.clientGetName().futureValue should be (empty)
      }
    }
  }
  
  Command.toString should {
    "return details about all redis commands" taggedAs (V2813) in {
      client.command().!
      client.command().futureValue should not be (empty)
    }
  }
  
  CommandCount.toString should {
    "return the total number of commands in this redis instance" taggedAs (V2813) in {
      client.commandCount().futureValue should be > (0)
    }
  }
  
  CommandGetKeys.toString when {
    "provided with garbage" should {
      "return an error" taggedAs (V2813) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.commandGetKeys("").!
        }
        a [RedisErrorResponseException] should be thrownBy {
          client.commandGetKeys("THIS IS GARBAGE").!
        }
      }
    }
    "provided with a command that has no keys" should {
      "return an empty list" taggedAs (V2813) ignore {
        client.commandGetKeys("PING").futureValue should be (empty)
      }
    }
    "provided with a command that has keys" should {
      "return them" taggedAs (V2813) ignore {
        client.commandGetKeys(
          """EVAL "not consulted" 3 key1  key2 key3 arg1      arg2 arg3 argN"""
        ).futureValue should be (empty)
      }
    }
  }
  
  scredis.protocol.requests.ServerRequests.CommandInfo.toString when {
    "provided with non-existent commands" should {
      "return an empty list" taggedAs (V2813) in {
        client.commandInfo("BULLSHIT").futureValue should be (empty)
      }
    }
    "provided with valid commands" should {
      "return the details about them" taggedAs (V2813) in {
        client.commandInfo("INFO", "PING", "AUTH").futureValue should have size (3)
      }
    }
    "provided with a mix of valid commands and non-existent commands" should {
      "return the details about the valid ones" taggedAs (V2813) in {
        client.commandInfo("INFO", "BULLSHIT", "PING", "AUTH", "").futureValue should have size (3)
      }
    }
  }

  ConfigGet.toString when {
    "providing an non-existent key" should {
      "return None" taggedAs (V200) in {
        client.configGet("thiskeywillneverexist").futureValue should be (empty)
      }
    }
    "not providing any pattern" should {
      "use * and return all" taggedAs (V200) in {
        client.configGet().futureValue should not be (empty)
      }
    }
    "providing a valid key that is empty" should {
      "return a None value" taggedAs (V200) in {
        client.configGet("requirepass").!("requirepass") should be (empty)
      }
    }
    "providing a valid key that is not empty" should {
      "return the key's value" taggedAs (V200) in {
        client.configGet("port").!("port") should be ("6379")
      }
    }
  }
  
  ConfigResetStat.toString should {
    "reset stats" taggedAs (V200) in {
      client.info().!("total_commands_processed").toInt should be > (1)
      client.configResetStat().futureValue should be (())
      client.info().!("total_commands_processed").toInt should be (1)
    }
  }
  
  ConfigRewrite.toString when {
    "the server is running without a config file" should {
      "return an error" taggedAs (V280) in {
        a [RedisErrorResponseException] should be thrownBy {
          client.configRewrite().!
        }
      }
    }
    "the server is running with a config file" should {
      "succeed" taggedAs (V280) in {
        client1.configRewrite().futureValue should be (())
      }
    }
  }

  ConfigSet.toString when {
    "providing a non-existent key" should {
      "return an error" taggedAs (V200) in {
        a [RedisErrorResponseException] should be thrownBy { 
          client.configSet("thiskeywillneverexist", "foo").!
        }
      }
    }
    "changing the password" should {
      "succeed" taggedAs (V200) in {
        client.configSet("requirepass", "guessit").futureValue should be (())
        a [RedisErrorResponseException] should be thrownBy {
          client.ping().!
        }
        client.auth("guessit").futureValue should be (())
        client.configGet("requirepass").!("requirepass") should be ("guessit")
        client.configSet("requirepass", "").futureValue should be (())
        client.configGet("requirepass").!("requirepass") should be (empty)
        client.auth("").futureValue should be (())
      }
    }
  }

  DBSize.toString should {
    "return the size of the current database" taggedAs (V100) in {
      client.select(0).futureValue should be (())
      client.flushDB().futureValue should be (())
      client.set("SOMEKEY", "SOMEVALUE")
      client.dbSize().futureValue should be (1)
      client.select(1).futureValue should be (())
      client.dbSize().futureValue should be (0)
      client.set("SOMEKEY", "SOMEVALUE")
      client.set("SOMEKEY2", "SOMEVALUE2")
      client.dbSize().futureValue should be (2)
    }
  }
  
  FlushAll.toString should {
    "flush all databases" taggedAs (V100) in {
      client.select(0).futureValue should be (())
      client.set("SOMEKEY", "SOMEVALUE")
      client.select(1).futureValue should be (())
      client.set("SOMEKEY", "SOMEVALUE")
      client.flushAll().futureValue should be (())
      client.dbSize().futureValue should be (0)
      client.select(0).futureValue should be (())
      client.dbSize().futureValue should be (0)
    }
  }

  FlushDB.toString should {
    "flush the current database only" taggedAs (V100) in {
      client.select(0).futureValue should be (())
      client.set("SOMEKEY", "SOMEVALUE")
      client.select(1).futureValue should be (())
      client.set("SOMEKEY", "SOMEVALUE")
      client.dbSize().futureValue should be (1)
      client.flushDB().futureValue should be (())
      client.dbSize().futureValue should be (0)
      client.select(0).futureValue should be (())
      client.dbSize().futureValue should be (1)
    }
  }

  Info.toString when {
    "not provided with any section" should {
      "return the default section" taggedAs (V100) in {
        val map = client.info().!
        map.contains("redis_version") should be (true)
        map.contains("connected_clients") should be (true)
        map.contains("used_memory") should be (true)
        map.contains("loading") should be (true)
        map.contains("total_connections_received") should be (true)
        map.contains("role") should be (true)
        map.contains("used_cpu_sys") should be (true)
      }
    }
    "provided with a non-existent section" should {
      "return an empty string/map" taggedAs (V260) in {
        client.info("YOU BET").futureValue should be (empty)
      }
    }
    "provided with a valid section" should {
      "only return the selected section" taggedAs (V260) in {
        val map = client.info("server").!
        map.contains("redis_version") should be (true)
        map.contains("connected_clients") should be (false)
      }
    }
  }

  LastSave.toString should {
    "return the UNIX timestamp of the last database save" taggedAs (V100) in {
      client.lastSave().futureValue should be > (0l)
    }
  }
  
  scredis.protocol.requests.ServerRequests.Role.toString when {
    "querying a master database" should {
      "return the master role" taggedAs (V2812) in {
        client1.slaveOf("127.0.0.1", 6379).futureValue should be (())
        Thread.sleep(2000)
        val role = client.role().futureValue
        role shouldBe a [scredis.Role.Master]
        inside(role) {
          case scredis.Role.Master(offset, slaves) => inside(slaves) {
            case scredis.Role.SlaveInfo(ip, port, offset) :: Nil => {
              ip should be ("127.0.0.1")
              port should be (6380)
            }
          }
        }
      }
    }
    "querying a slave database" should {
      "return the slave role" taggedAs (V2812) in {
        val role = client1.role().futureValue
        role shouldBe a [scredis.Role.Slave]
        inside(role) {
          case scredis.Role.Slave(masterIp, masterPort, state, offset) => {
            masterIp should be ("127.0.0.1")
            masterPort should be (6379)
          }
        }
        client1.slaveOfNoOne().futureValue should be (())
        Thread.sleep(2000)
        client1.role().futureValue shouldBe a [scredis.Role.Master]
      }
    }
  }

  Save.toString should {
    "save the database" taggedAs (V100) in {
      client.save().futureValue should be (())
    }
  }
  
  SlowLogGet.toString when {
    "count is not specified" should {
      "return all slowlog entries" taggedAs (V2212) in {
        val entries = client.slowLogGet().!
        entries.size should be >= (0)
      }
    }
    "count is specified" should {
      "return at most count slowlog entries" taggedAs (V2212) in {
        val entries = client.slowLogGet(countOpt = Some(3)).!
        entries.size should be >= (0)
        entries.size should be <= (3)
      }
    }
  }
  
  SlowLogLen.toString should {
    "return the size of the slowlog" taggedAs (V2212) in {
      client.slowLogLen().futureValue should be >= (0l)
    }
  }
  
  SlowLogReset.toString should {
    "succeed" taggedAs (V2212) in {
      client.slowLogReset().futureValue should be (())
    }
  }
  
  Time.toString should {
    "return the current server UNIX timestamp with microseconds" taggedAs (V260) in {
      val (seconds, microseconds) = client.time().futureValue
      seconds should be > (0l)
      microseconds should be >= (0l)
    }
  }
  
  override def afterAll(): Unit = {
    client.flushAll().!
    client.quit().!
    client1.flushAll().!
    client1.quit().!
  }
  
}
