package scredis.serialization

import org.scalatest._
import org.scalatest.concurrent._

import scredis._
import scredis.exceptions._
import scredis.util.TestUtils._
import scredis.tags._

class SerializationSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  import Implicits._
  
  private val Bytes = {
    val array = new Array[Byte](4)
    for(i <- 0 to 3) array(i) = i.toByte
    array
  }
  private val Value = "Hello World! @#%*^!:/'asd}!虫àéç蟲"
    
  private val client = Client()
  
  "Writing" when {
    "values are stored with different types" should {
      "correctly parse various types" taggedAs(V100) in {
        client.set[Array[Byte]]("bytes", Bytes).futureValue should be (true)
        client.set[Array[Byte]]("utf-16", Value.getBytes("UTF-16")).futureValue should be (true)
        client.set[String]("str", Value).futureValue should be (true)
        client.set[Boolean]("boolean", true).futureValue should be (true)
        client.set[Int]("number", 5).futureValue should be (true)
        client.set[Double]("decimal", 5.5).futureValue should be (true)
        client.rPush[Any]("list", "1", 2, 3, "4", "5").futureValue should be (5)
      }
    }
  }
  
  "Reading" when {
    "values are stored with different types" should {
      "correctly parse various types" taggedAs(V100) in {
        client.get[Array[Byte]]("bytes").futureValue.get.sameElements(Bytes) should be (true)
        client.get[Array[Byte]]("utf-16").futureValue.map(
          new String(_, "UTF-16")
        ) should contain (Value)
        client.get[String]("utf-16").futureValue should not contain (Value)
        client.get[String]("str").futureValue should contain (Value)
        client.get[Boolean]("boolean").futureValue should contain (true)
        client.get[Short]("number").futureValue should contain (5)
        client.get[Int]("number").futureValue should contain (5)
        client.get[Long]("number").futureValue should contain (5L)
        client.get[Float]("decimal").futureValue should contain (5.5)
        client.get[Double]("decimal").futureValue should contain (5.5)
        client.lRange[Int]("list").futureValue should contain theSameElementsInOrderAs List(
          1, 2, 3, 4, 5
        )
        a [RedisReaderException] should be thrownBy {
          client.get[Boolean]("decimal").!
        }
        a [RedisReaderException] should be thrownBy {
          client.get[Long]("decimal").!
        }
        a [RedisReaderException] should be thrownBy {
          client.lRange[Boolean]("list").!
        }
      }
    }
  }
  
  override def afterAll() {
    client.flushAll().!
    client.quit().!
  }

}