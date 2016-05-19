package scredis.protocol

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{Matchers, WordSpec}

class ClusterCRC16Spec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks with TableDrivenPropertyChecks {

  val examples = Table(
    ("input", "output"),
    ("", 0x0),
    ("123456789", 0x31C3),
    ("sfger132515", 0xA45C),
    ("hae9Napahngaikeethievubaibogiech", 0x58CE),
    ("AAAAAAAAAAAAAAAAAAAAAA", 0x92cd),
    ("Hello, World!", 0x4FD6)
  )

  "getSlot" should {
    "yield the same hash for a tag within a string as for the plain tag" in {
      forAll { (tag: String, left:String, right:String) =>
        whenever(!left.contains("{") && tag.nonEmpty && !tag.contains("}")) {
          val key = s"$left{$tag}$right"
          ClusterCRC16.getSlot(key) should be(ClusterCRC16.getSlot(tag))
        }
      }
    }

    "solve all examples correctly" in {
      forAll (examples) { (in:String, out:Int) =>
        ClusterCRC16.getCRC16(in) shouldBe out
      }
    }
  }



}
