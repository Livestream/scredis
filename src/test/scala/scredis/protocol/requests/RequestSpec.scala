package scredis.protocol.requests

import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scredis.ClusterNode

/**
 * Created by justin on 21.04.15.
 */
class RequestSpec extends WordSpec with GeneratorDrivenPropertyChecks with Inside
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers {

  "bla" should {
    "decode example" in {
      val example = """389afe686935b70115e1efd4f29df552569340a2 127.0.0.1:7004 slave 8d7ef440da8196d56150313f86c4b0be90aa9501 0 1429605082642 5 connected
                      |623f66a717d77220a720a325d1a72ead108794e4 127.0.0.1:7003 slave 2593da8ffa32710544119ca8cb42a88c566589d3 0 1429605082141 4 connected
                      |8d7ef440da8196d56150313f86c4b0be90aa9501 127.0.0.1:7002 master - 0 1429605081639 3 connected 5461-10922
                      |2593da8ffa32710544119ca8cb42a88c566589d3 127.0.0.1:7000 myself,master - 0 0 1 connected 0-5460 22 1024-3333
                      |789684c5046734068810407e2e9b282c16ca18f2 127.0.0.1:7001 master - 0 1429605080638 2 connected 10923-16383
                      |bad2c77fd9e8091f2deda8a5289ae8a473d957a8 127.0.0.1:7005 slave 789684c5046734068810407e2e9b282c16ca18f2 0 1429605082642 6 connected
                      |""".stripMargin

      val res = ClusterRequests.parseClusterNodes(example)

      res should have length(6)

      inside (res(3)) {
        case ClusterNode(
        "2593da8ffa32710544119ca8cb42a88c566589d3",
        "127.0.0.1",7000, flags,
        None, 0, 0, 1, true,
        slots) =>
          flags should contain theSameElementsAs Seq("myself","master")
          slots should contain theSameElementsAs Seq((0l,5460l),(22l,22l),(1024l,3333l))
        case _ => fail
      }

    }
  }

}
