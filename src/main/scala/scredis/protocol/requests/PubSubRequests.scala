package scredis.protocol.requests

import scredis.PubSubMessage
import scredis.protocol._
import scredis.exceptions.RedisProtocolException
import scredis.serialization.Writer

import scala.collection.generic.CanBuildFrom

object PubSubRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.intReader
  
  object PSubscribe extends Command("PSUBSCRIBE")
  object Publish extends Command("PUBLISH") with WriteCommand
  object PubSubChannels extends Command("PUBSUB", "CHANNELS")
  object PubSubNumSub extends Command("PUBSUB", "NUMSUB")
  object PubSubNumPat extends ZeroArgCommand("PUBSUB", "NUMPAT")
  object PUnsubscribe extends Command("PUNSUBSCRIBE")
  object Subscribe extends Command("SUBSCRIBE")
  object Unsubscribe extends Command("UNSUBSCRIBE")
  
  case class PSubscribe(patterns: String*) extends Request[Int](
    PSubscribe, patterns: _*
  ) {
    override def decode = ???
  }
  
  case class Publish[W: Writer](channel: String, message: W) extends Request[Long](
    Publish, channel, implicitly[Writer[W]].write(message)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class PubSubChannels[CC[X] <: Traversable[X]](patternOpt: Option[String])(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
  ) extends Request[CC[String]](PubSubChannels, patternOpt.toSeq: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class PubSubNumSub[CC[X, Y] <: collection.Map[X, Y]](channels: String*)(
    implicit cbf: CanBuildFrom[Nothing, (String, Int), CC[String, Int]]
  ) extends Request[CC[String, Int]](PubSubNumSub, channels: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, Int, CC] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[Int]
      }
    }
  }
  
  case class PubSubNumPat() extends Request[Long](PubSubNumPat) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class PUnsubscribe(patterns: String*) extends Request[Int](
    PUnsubscribe, patterns: _*
  ) {
    override def decode = ???
  }
  
  case class Subscribe(channels: String*) extends Request[Int](
    Subscribe, channels: _*
  ) {
    override def decode = ???
  }
  
  case class Unsubscribe(channels: String*) extends Request[Int](
    Unsubscribe, channels: _*
  ) {
    override def decode = ???
  }
  
}