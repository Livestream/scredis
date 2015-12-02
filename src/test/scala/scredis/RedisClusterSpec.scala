package scredis

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Functionality test of Cluster client.
 */
class RedisClusterSpec extends WordSpec
  with Matchers
  with ScalaFutures
  with GeneratorDrivenPropertyChecks {

  val keys = org.scalacheck.Arbitrary.arbString.arbitrary

  // we assume there is a local cluster started on ports 7000 - 7005
  // see testing.md
  val cluster = RedisCluster(Server("localhost",7000))

  "connection to cluster" should {
    "work for a single valid seed node" in {
      val info = cluster.clusterInfo().futureValue

      info("cluster_state") should be ("ok")
      info("cluster_known_nodes").toInt should be (6) // 6 total nodes
      info("cluster_size").toInt should be (3) // 3 master nodes
    }

    "work when some of the seed nodes are offline" in {
      val badSeeds = RedisCluster(Server("localhost",7777), Server("localhost",2302), Server("localhost",7003))

      val info = badSeeds.clusterInfo().futureValue
      info("cluster_state") should be ("ok")
      info("cluster_known_nodes").toInt should be (6) // 6 total nodes
      info("cluster_size").toInt should be (3) // 3 master nodes
    }
  }

  "writes to cluster" should {
    "be readable" in {
      forAll { (key:String, value: String) =>
        whenever (value.nonEmpty) {
          val res = for {
            _ <- cluster.set(key, value)
            g <- cluster.get(key)
          } yield g.get
          res.futureValue should be(value)
        }
      }
    }

    "be idempotent" in {
      forAll { (key:String, value: String) =>
        whenever (value.nonEmpty) {
          val res = for {
            _ <- cluster.set(key, value)
            g1 <- cluster.get(key)
            _ <- cluster.set(key, value)
            g2 <- cluster.get(key)
          } yield (g1.get,g2.get)
          res.futureValue should be(value,value)
        }
      }
    }
  }


  // TODO basic test for each supported / unsupported command

}
