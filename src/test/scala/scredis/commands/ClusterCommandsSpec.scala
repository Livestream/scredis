package scredis.commands

import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import scredis.exceptions.{RedisErrorResponseException, RedisProtocolException}
import scredis.protocol.Protocol
import scredis.protocol.requests.ClusterRequests._
import scredis.{ClusterSlotRange, Server, RedisCluster}
import scala.concurrent.duration._


class ClusterCommandsSpec extends WordSpec
  with Matchers
  with ScalaFutures
  with GeneratorDrivenPropertyChecks {


  val cluster = RedisCluster(Server("localhost",7000))
  import cluster.dispatcher

  val slots = Gen.choose(0l, Protocol.CLUSTER_HASHSLOTS.toLong - 1)

  // TODO get all the node ids
  //  cluster.clusterNodes()

  // informational commands

  ClusterKeyslot.toString should {
    "return a valid slot for any key" in {
        forAll { (key: String) =>
        val slot = cluster.clusterKeyslot(key).futureValue
        assert( slot >= 0 && slot < Protocol.CLUSTER_HASHSLOTS)
      }
    }
  }

  ClusterCountFailureReports.toString should {
    "return a number >= 0 for all nodes" in {
//      cluster.clusterNodes()
      fail // TODO need the node ids for this
    }
  }

  ClusterGetKeysInSlot.toString should {
    // TODO the command is node-specific. Find a better test, or implement s.t. it talks to the correct node.
    "succeed" in {
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
    "return node information" in {
      cluster.clusterNodes().futureValue should not be empty
      // TODO better test
    }
  }

  ClusterSlaves.toString when {
    "a master's id is known" should {
      "return slave information" in {
  //      cluster.clusterSlaves()
        // TODO need node information to test this
        fail
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
      val fullrange = slots
        .map { case ClusterSlotRange(range,_,_) => range }
        .sorted.foldLeft((0l,0l)) { case((low,high),(begin,end)) => if (begin<=high+1) (low,end) else (low,high)  }
      //         ^ does a kind of merge on the value ranges

      fullrange should be ((0, Protocol.CLUSTER_HASHSLOTS-1))
    }
  }
}
