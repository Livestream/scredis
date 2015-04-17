package scredis

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import scredis.io.Server

import scala.concurrent.ExecutionContext.Implicits.global

class RedisClusterSpec extends WordSpec
  with Matchers
  with ScalaFutures
  with GeneratorDrivenPropertyChecks {

  val keys = org.scalacheck.Arbitrary.arbString.arbitrary

  // we assume there is a local cluster started on ports 7000 - 7005
  val cluster = RedisCluster(Server("localhost",7000) :: Nil)

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

}
