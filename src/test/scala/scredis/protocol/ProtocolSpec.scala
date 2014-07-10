package scredis.protocol

import org.scalatest._
import org.scalatest.concurrent._

import akka.util.ByteString

class ProtocolSpec extends WordSpec
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
  
}
