package scredis.protocol


import akka.util.ByteString
import org.scalatest._
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ProtocolSpec extends WordSpec with GeneratorDrivenPropertyChecks with Inside
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers {
  
  private val ArrayString = "*5\r\n-error\r\n+simple\r\n:1000\r\n:-1000\r\n$3\r\nlol\r\n"
  
  private val Error = ByteString("-error\r\n")
  private val SimpleString = ByteString("+simple\r\n")
  private val PositiveInteger = ByteString(":1000\r\n")
  private val NegativeInteger = ByteString(":-1000\r\n")
  private val BulkString = ByteString("$3\r\nlol\r\n")
  private val Array = ByteString(ArrayString)
  private val All = Error ++
    SimpleString ++
    PositiveInteger ++
    NegativeInteger ++
    BulkString ++
    Array

  "count" when {
    "receiving different types of response" should {
      "correctly count them" in {
        val buffer = All.toByteBuffer
        Protocol.count(buffer) should be (6)
        buffer.remaining should be (0)
        val buffer2 = ByteString("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n").toByteBuffer
        Protocol.count(buffer2) should be (1)
        buffer2.remaining should be (0)
      }
    }
    "receiving different types of responses with nested arrays" should {
      "correctly count them" in {
        val nestedArrays = All ++ ByteString(
          s"*3\r\n*2\r\n$ArrayString$ArrayString*2\r\n$ArrayString-error\r\n$$3\r\nlol\r\n"
        )
        val buffer = nestedArrays.toByteBuffer
        Protocol.count(buffer) should be (7)
        buffer.remaining should be (0)
      }
    }
    "receiving fragmented responses" should {
      "count them up to the last full response" in {
        var fragmented = ByteString("").toByteBuffer
        Protocol.count(fragmented) should be (0)
        fragmented.remaining should be (0)
        
        fragmented = ByteString("-").toByteBuffer
        Protocol.count(fragmented) should be (0)
        fragmented.position should be (0)
        
        fragmented = ByteString("-error").toByteBuffer
        Protocol.count(fragmented) should be (0)
        fragmented.position should be (0)
        
        fragmented = ByteString("-error\r").toByteBuffer
        Protocol.count(fragmented) should be (0)
        fragmented.position should be (0)
        
        fragmented = ByteString("-error\r\n+hello").toByteBuffer
        Protocol.count(fragmented) should be (1)
        fragmented.position should be (8)
        
        fragmented = ByteString(
          "*5\r\n-error\r\n+simple\r\n:1000\r\n:-1000\r\n$3\r\nlol\r"
        ).toByteBuffer
        Protocol.count(fragmented) should be (0)
        fragmented.position should be (0)
        
        fragmented = ByteString(
          s"$ArrayString*3\r\n*2\r\n$ArrayString$ArrayString*2\r\n$ArrayString-error\r\n$$3\r\n"
        ).toByteBuffer
        Protocol.count(fragmented) should be (1)
        fragmented.position should be (ArrayString.size)
      }
    }
  }
  
  "decode" should {
    "succeed" in {
      val buffer = ByteString("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n").toByteBuffer
      val response = Protocol.decode(buffer)
      response shouldBe an [ArrayResponse]
      val arrayResponse = response.asInstanceOf[ArrayResponse]
      arrayResponse.length should be (3)
    }
  }

  val hosts = for {
    a <- Gen.choose(0,255)
    b <- Gen.choose(0,255)
    c <- Gen.choose(0,255)
    d <- Gen.choose(0,255)
  } yield s"$a.$b.$c.$d"

  val ports = Gen.choose(1,65535)
  val slots = Gen.choose(0,Protocol.CLUSTER_HASHSLOTS)

  "decodeMoveAsk" should {
    "extract slot, host and port from MOVED error" in {
      forAll (slots,hosts,ports) { (slot,host,port) =>
        val cmd = "MOVED"
        val args = s"$slot $host:$port"
        val Moved(slot1,host1,port1) = Protocol.decodeMoveAsk(cmd,args)
        slot1 should be (slot)
        host1 should be (host)
        port1 should be (port)
      }
    }

    "extract slot, host and port from ASK error" in {
      forAll (slots,hosts,ports) { (slot,host,port) =>
        val cmd = "ASK"
        val args = s"$slot $host:$port"
        val Ask(slot1,host1,port1) = Protocol.decodeMoveAsk(cmd,args)
        slot1 should be (slot)
        host1 should be (host)
        port1 should be (port)
      }
    }
  }

  "decodeError" should {
    "decode a MOVED error" in {
      forAll (slots,hosts,ports) { (slot, host, port) =>
        val errMsg = s"MOVED $slot $host:$port"
        val err = Protocol.decodeError(errMsg)
        inside (err) { case (ClusterErrorResponse(Moved(s, h, p), msg)) =>
          s should be (slot)
          h should be (host)
          p should be (port)
          msg should include ("MOVED")
        }
      }
    }
  }

}
