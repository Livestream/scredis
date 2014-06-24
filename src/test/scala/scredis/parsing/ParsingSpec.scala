package scredis.parsing

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import scredis.Client
import scredis.exceptions.RedisParsingException
import scredis.tags._
import scredis.parsing.Implicits._

class ParsingSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val Bytes = {
    val array = new Array[Byte](4)
    for(i <- 0 to 3) array(i) = i.toByte
    array
  }
  private val Value = "Hello World! @#%*^!:/'asd}!虫àéç蟲"
    
  private val client = Client()
  
  override def beforeAll() {
    client.set("bytes", Bytes)
    client.set("utf-16", Value.getBytes("UTF-16"))
    client.set("str", Value)
    client.set("boolean", true)
    client.set("number", 5)
    client.set("decimal", 5.5)
    client.rPush("list", "1", 2, 3, "4", "5")
  }
  
  "Parsing" when {
    "values are stored with different types" should {
      "correctly parse various types" taggedAs V100 in {
        client.get[Array[Byte]]("bytes").get.sameElements(Bytes) must be(true)
        client.get[Array[Byte]]("utf-16").map(new String(_, "UTF-16")) must be(Some(Value))
        client.get[String]("utf-16") must not be(Some(Value))
        client.get[String]("str") must be(Some(Value))
        client.get[Boolean]("boolean") must be(Some(true))
        client.get[Short]("number") must be(Some(5))
        client.get[Int]("number") must be(Some(5))
        client.get[Long]("number") must be(Some(5L))
        client.get[Float]("decimal") must be(Some(5.5))
        client.get[Double]("decimal") must be(Some(5.5))
        client.lRange[Int]("list") must be(List(1, 2, 3, 4, 5))
        evaluating { client.get[Boolean]("decimal") } must produce[RedisParsingException]
        evaluating { client.get[Long]("decimal") } must produce[RedisParsingException]
        evaluating { client.lRange[Boolean]("list") } must produce[RedisParsingException]
      }
    }
  }
  
  override def afterAll() {
    client.flushAll()
    client.quit()
  }

}