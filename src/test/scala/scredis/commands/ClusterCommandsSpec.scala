package scredis.commands

import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{Matchers, WordSpec}
import scredis.exceptions.RedisErrorResponseException
import scredis.protocol.Protocol
import scredis.protocol.requests.ClusterRequests._
import scredis.{ClusterSlotRange, RedisCluster, Server}

import scala.concurrent.duration._


class ClusterCommandsSpec extends WordSpec
  with Matchers
  with ScalaFutures
  with GeneratorDrivenPropertyChecks
  with TableDrivenPropertyChecks {


  val cluster = RedisCluster(Server("localhost",7000))

  val slots = Gen.choose(0l, Protocol.CLUSTER_HASHSLOTS.toLong - 1)

  // informational commands

  ClusterKeyslot.toString should {
    "return a valid slot for any key" in {
      forAll { (key: String) =>
        val slot = cluster.clusterKeyslot(key).futureValue
        assert( slot >= 0 && slot < Protocol.CLUSTER_HASHSLOTS)
      }
    }
  }

  ClusterCountFailureReports.toString when {
    "node ids are available" should {
      val nodes = cluster.clusterNodes().futureValue
      val nodeIds = Table("node", nodes.map{ node => node.nodeId}: _*)

      "return a number >= 0 for all nodes" in {
        forAll(nodeIds) { id =>
          val failures = cluster.clusterCountFailureReports(id).futureValue
          failures should be >= 0l
        }
      }
    }
  }

  ClusterGetKeysInSlot.toString should {
    // TODO the command is node-specific. Find a better test, or implement s.t. it talks to the correct node.
    "succeed" ignore {
      forAll(slots) { slot: Long =>
        val keysInSlot = cluster.clusterGetKeysInSlot(slot,1)
        keysInSlot.isReadyWithin(1.seconds)
      }
    }
  }

  ClusterInfo.toString should {
    "contain expected information" in {
      val info = cluster.clusterInfo().futureValue
      info("cluster_state") should be ("ok")
    }
  }

  ClusterNodes.toString should {
    "return node information with as many masters and slaves as contained in" in {
      val info = cluster.clusterInfo().futureValue
      val nodes = cluster.clusterNodes().futureValue
      nodes should not be empty
      val slaveCount = nodes.count { node => node.flags.contains("slave")}
      val masterCount = nodes.size - slaveCount

      val clusterSize = info("cluster_size").toInt
      masterCount should be (clusterSize)
      slaveCount should be (info("cluster_known_nodes").toInt - clusterSize)
    }
  }

  ClusterSlaves.toString when {
    "a master's id is known" should {

      val nodes = cluster.clusterNodes().futureValue
      val masterNodes = nodes.filter { node => node.flags.contains("master") }
      val masters = Table("master", masterNodes:_*)

      "return slave information" in {
        forAll(masters) { master =>
          val slaves = cluster.clusterSlaves(master.nodeId).futureValue
          assert( slaves.forall { slave => slave.master.get == master.nodeId } )
        }
      }
    }

    "using an arbitrary id" should {
      "return an error" in {
        forAll { node: String =>
          whenReady(cluster.clusterSlaves(node).failed) { e =>
            e shouldBe a [RedisErrorResponseException]
            e.getMessage should include ("Unknown node")
          }
        }
      }
    }
  }

  ClusterSlots.toString should {
    "cover all slots" in {
      val slots = cluster.clusterSlots().futureValue
      val fullRange = slots
        .map { case ClusterSlotRange(range,_,_) => range }
        .sorted.foldLeft((0l,0l)) { case((low,high),(begin,end)) => if (begin<=high+1) (low,end) else (low,high)  }
      //         ^ does a kind of merge on the value ranges

      fullRange should be ((0, Protocol.CLUSTER_HASHSLOTS-1))
    }
  }

  // effecty commands elided ... they are somewhat harder to test properly because they change cluster state
}
