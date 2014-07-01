package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object SetRequests {
  
  object SAdd extends Command("SADD")
  object SCard extends Command("SCARD")
  object SDiff extends Command("SDIFF")
  object SDiffStore extends Command("SDIFFSTORE")
  object SInter extends Command("SINTER")
  object SInterStore extends Command("SINTERSTORE")
  object SIsMember extends Command("SISMEMBER")
  object SMembers extends Command("SMEMBERS")
  object SMove extends Command("SMOVE")
  object SPop extends Command("SPOP")
  object SRandMember extends Command("SRANDMEMBER")
  object SRem extends Command("SREM")
  object SScan extends Command("SSCAN")
  object SUnion extends Command("SUNION")
  object SUnionStore extends Command("SUNIONSTORE")
  
  case class SAdd[W: Writer](key: String, members: W*) extends Request[Long](
    SAdd, key, members.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SCard(key: String) extends Request[Long](SCard, key) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SDiff[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SDiff, keys: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SDiffStore(destination: String, keys: String*) extends Request[Long](
    SDiffStore, destination, keys: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SInter[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SInter, keys: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SInterStore(destination: String, keys: String*) extends Request[Long](
    SInterStore, destination, keys: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SIsMember[W: Writer](key: String, member: W) extends Request[Boolean](
    SIsMember, key, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SMembers[R: Reader, CC[X] <: Traversable[X]](key: String)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SMembers, key) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SMove[W: Writer](
    source: String, destination: String, member: W
  ) extends Request[Boolean](
    SMove, source, destination, implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SPop[R: Reader](key: String) extends Request[Option[R]](SPop, key) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMember[R: Reader](key: String) extends Request[Option[R]](SRandMember, key) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMembers[R: Reader, CC[X] <: Traversable[X]](key: String, count: Int)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SRandMember, key, count) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SRem[W: Writer](key: String, members: W*) extends Request[Long](
    SRem, key, members.map(implicitly[Writer[W]].write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SScan[R: Reader, CC[X] <: Traversable[X]](
    key: String,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(implicit cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[(Long, CC[R])](
    SScan,
    generateScanLikeArgs(
      keyOpt = Some(key),
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    ): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[R, CC] {
        case a: ArrayResponse => a.parsed[R, CC] {
          case b: BulkStringResponse => b.flattened[R]
        }
      }
    }
  }
  
  case class SUnion[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SUnion, keys: _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SUnionStore(destination: String, keys: String*) extends Request[Long](
    SUnionStore, destination, keys: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
}