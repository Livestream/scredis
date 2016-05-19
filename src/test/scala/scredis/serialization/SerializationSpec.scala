package scredis.serialization

import java.util.UUID

import org.scalatest._
import org.scalatest.concurrent._
import scredis._
import scredis.exceptions._
import scredis.tags._
import scredis.util.TestUtils._

class SerializationSpec extends WordSpec
  with GivenWhenThen
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {
  
  import scredis.serialization.Implicits._
  
  private val Bytes = {
    val array = new Array[Byte](4)
    for(i <- 0 to 3) array(i) = i.toByte
    array
  }
  private val Value = "Hello World! @#%*^!:/'asd}!虫àéç蟲"
  private val Uuid = UUID.fromString("de305d54-75b4-431b-adb2-eb6b9e546013")
    
  private val client = Client()
  
  "Writing" when {
    "values are stored with different types" should {
      "correctly parse various types" taggedAs(V100) in {
        client.set[String, Array[Byte]]("bytes", Bytes).futureValue should be (true)
        client.set[String, Array[Byte]]("utf-16", Value.getBytes("UTF-16")).futureValue should be (true)
        client.set[String, String]("str", Value).futureValue should be (true)
        client.set[String, Boolean]("boolean", true).futureValue should be (true)
        client.set[String, Int]("number", 5).futureValue should be (true)
        client.set[String, Double]("decimal", 5.5).futureValue should be (true)
        client.set[String, UUID]("uuid", Uuid).futureValue should be (true)
        client.rPush[String, Any]("list", "1", 2, 3, "4", "5")(UTF8StringWriter, AnyWriter).futureValue should be (5)
      }
    }
    "keys are stored with different types" should {
      "correctly parse various types" taggedAs(V100) in {
        client.set[Array[Byte], String](Bytes, Value).futureValue should be(true)
        client.set[Array[Byte], String](Value.getBytes("UTF-16"), Value).futureValue should be(true)
        client.set[String, String]("str", Value).futureValue should be(true)
        client.set[Boolean, String](true, Value).futureValue should be(true)
        client.set[Int, String](5, Value).futureValue should be(true)
        client.set[Double, String](5.5, Value).futureValue should be(true)
        client.set[UUID, String](Uuid, Value).futureValue should be(true)
        client.set[Any, String](5, Value)(AnyWriter, UTF8StringWriter).futureValue should be(true)
      }
    }
  }
  
  "Reading" when {
    "values are stored with different types" should {
      "correctly parse various types" taggedAs(V100) in {
        client.get[String, Array[Byte]]("bytes").futureValue.get.sameElements(Bytes) should be (true)
        client.get[String, Array[Byte]]("utf-16").futureValue.map(
          new String(_, "UTF-16")
        ) should contain (Value)
        client.get[String, String]("utf-16").futureValue should not contain (Value)
        client.get[String, String]("str").futureValue should contain (Value)
        client.get[String, Boolean]("boolean").futureValue should contain (true)
        client.get[String, Short]("number").futureValue should contain (5)
        client.get[String, Int]("number").futureValue should contain (5)
        client.get[String, Long]("number").futureValue should contain (5L)
        client.get[String, Float]("decimal").futureValue should contain (5.5)
        client.get[String, Double]("decimal").futureValue should contain (5.5)
        client.get[String, UUID]("uuid").futureValue should contain (Uuid)
        client.get[String, UUID]("str").futureValue should contain (null)
        client.lRange[String, Int]("list").futureValue should contain theSameElementsInOrderAs List(
          1, 2, 3, 4, 5
        )
        a [RedisReaderException] should be thrownBy {
          client.get[String, Boolean]("decimal").!
        }
        a [RedisReaderException] should be thrownBy {
          client.get[String, Long]("decimal").!
        }
        a [RedisReaderException] should be thrownBy {
          client.lRange[String, Boolean]("list").!
        }
      }
    }
    "keys are stored with different types" should {
      "correctly parse various types" taggedAs (V100) in {
        client.get[Array[Byte], String](Bytes).futureValue should contain(Value)
        client.get[Array[Byte], String](Value.getBytes("UTF-16")).futureValue should contain(Value)
        client.get[String, String]("str").futureValue should contain(Value)
        client.get[Boolean, String](true).futureValue should contain(Value)
        client.get[Int, String](5).futureValue should contain(Value)
        client.get[Double, String](5.5).futureValue should contain(Value)
        client.get[UUID, String](Uuid).futureValue should contain(Value)
        client.get[Any, String](5)(AnyWriter, UTF8StringReader).futureValue should contain(Value)
      }
    }
  }
  
  override def afterAll() {
    client.flushAll().!
    client.quit().!
  }

}