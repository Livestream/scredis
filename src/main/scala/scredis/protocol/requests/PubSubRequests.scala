package scredis.protocol.requests

import scredis.PubSubMessage
import scredis.protocol._
import scredis.exceptions.RedisProtocolException
import scredis.serialization.Writer

import scala.collection.generic.CanBuildFrom

object PubSubRequests {
  
  import scredis.serialization.Implicits.stringReader
  
  object PSubscribe extends Command("PSUBSCRIBE") {
    override def isSubscriber = true
  }
  object Publish extends Command("PUBLISH")
  object PubSubChannels extends Command("PUBSUB", "CHANNELS")
  object PubSubNumSub extends Command("PUBSUB", "NUMSUB")
  object PubSubNumPat extends ZeroArgCommand("PUBSUB", "NUMPAT")
  object PUnsubscribe extends Command("PUNSUBSCRIBE") {
    override def isSubscriber = true
  }
  object Subscribe extends Command("SUBSCRIBE") {
    override def isSubscriber = true
  }
  object Unsubscribe extends Command("UNSUBSCRIBE") {
    override def isSubscriber = true
  }
  
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
    implicit cbf: CanBuildFrom[Nothing, (String, Long), CC[String, Long]]
  ) extends Request[CC[String, Long]](PubSubNumSub, channels: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, Long, CC] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case IntegerResponse(value) => value
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