package scredis.io

import org.scalatest.{ WordSpec, GivenWhenThen }
import org.scalatest.matchers.MustMatchers._

class JavaSocketConnectionSpec extends WordSpec with GivenWhenThen {
  private val connection = new JavaSocketConnection("localhost", 6379)

  "A connection" when {
    "just initialized" should {
      "not be connected" in {
        connection.isConnected must be(false)
      }
    }
    "connected" should {
      "be connected" in {
        connection.connect()
        connection.isConnected must be(true)
      }
    }
    "disconnected" should {
      "no longer be connected" in {
        connection.disconnect()
        connection.isConnected must be(false)
      }
    }
    "reconnected" should {
      "be connected" in {
        connection.connect()
        connection.reconnect()
        connection.isConnected must be(true)
      }
    }
    "writing bytes of a basic SET command" should {
      "not produce any error" in {
        connection.write(
          ("*3\r\n" +
            "$3\r\n" +
            "SET\r\n" +
            "$5\r\n" +
            "mykey\r\n" +
            "$7\r\n" +
            "myvalue\r\n").getBytes("UTF-8"))
      }
    }
    "reading the server's reply" should {
      "get the correct status value" in {
        val statusReply = new String(connection.readLine(), "UTF-8").tail
        statusReply must equal("OK")
      }
    }
    "writing bytes of a basic GET command" should {
      "not produce any error" in {
        connection.write(
          ("*2\r\n" +
            "$3\r\n" +
            "GET\r\n" +
            "$5\r\n" +
            "mykey\r\n").getBytes("UTF-8"))
      }
    }
    "reading the server's reply" should {
      "get the correct value" in {
        val count = new String(connection.readLine(), "UTF-8").tail.toInt
        val reply = new String(connection.read(count), "UTF-8")
        reply must equal("myvalue")
      }
    }
    "trying to read an empty input stream" should {
      "timeout" in {
        evaluating { connection.readLine() } must produce[ConnectionTimeoutException]
      }
    }
    "disconnecting" should {
      "no longer be connected" in {
        connection.write((
          "*1\r\n" +
          "$8\r\n" +
          "FLUSHALL\r\n"
        ).getBytes("UTF-8"))
        new String(connection.readLine(), "UTF-8").tail must be("OK")
        connection.write((
          "*1\r\n" +
          "$4\r\n" +
          "QUIT\r\n"
        ).getBytes("UTF-8"))
        new String(connection.readLine(), "UTF-8").tail must be("OK")
        connection.disconnect()
        connection.isConnected must be(false)
      }
    }
  }

}