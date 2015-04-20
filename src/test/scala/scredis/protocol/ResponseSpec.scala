package scredis.protocol

import java.nio.ByteBuffer

import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scredis.ClusterSlotRange

class ResponseSpec extends WordSpec with GeneratorDrivenPropertyChecks with Inside
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers {

  "ArrayResponse" when {
    "parsedAsClusterSlotsResponse" should {
      "correctly decode an example" in {
        val bytes = "*3\r\n*4\r\n:5461\r\n:10922\r\n*2\r\n$9\r\n127.0.0.1\r\n:7002\r\n*2\r\n$9\r\n127.0.0.1\r\n:7004\r\n*4\r\n:0\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n*2\r\n$9\r\n127.0.0.1\r\n:7003\r\n*4\r\n:10923\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:7001\r\n*2\r\n$9\r\n127.0.0.1\r\n:7005\r\n".getBytes(Protocol.Encoding)
        val response = Protocol.decode(ByteBuffer.wrap(bytes)).asInstanceOf[ArrayResponse]
        val parsed = response.parsedAsClusterSlotsResponse[Vector]
        println(parsed)
        parsed should have size (3)
        parsed.head should be (ClusterSlotRange((5461,10922),("127.0.0.1",7002l),List(("127.0.0.1",7004l))))
      }
    }
  }

}
