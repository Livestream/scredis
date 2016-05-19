package scredis.protocol.requests

import scala.language.higherKinds
import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom

object SetRequests {
  
  object SAdd extends Command("SADD") with WriteCommand
  object SCard extends Command("SCARD")
  object SDiff extends Command("SDIFF")
  object SDiffStore extends Command("SDIFFSTORE") with WriteCommand
  object SInter extends Command("SINTER")
  object SInterStore extends Command("SINTERSTORE") with WriteCommand
  object SIsMember extends Command("SISMEMBER")
  object SMembers extends Command("SMEMBERS")
  object SMove extends Command("SMOVE") with WriteCommand
  object SPop extends Command("SPOP") with WriteCommand
  object SRandMember extends Command("SRANDMEMBER")
  object SRem extends Command("SREM") with WriteCommand
  object SScan extends Command("SSCAN")
  object SUnion extends Command("SUNION")
  object SUnionStore extends Command("SUNIONSTORE") with WriteCommand
  
  case class SAdd[W](key: String, members: W*)(implicit writer: Writer[W]) extends Request[Long](
    SAdd, key +: members.map(writer.write): _*
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SCard(key: String) extends Request[Long](SCard, key) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SDiff[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SDiff, keys: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
    override val key = keys.head
  }
  
  case class SDiffStore(destination: String, keys: String*) extends Request[Long](
    SDiffStore, destination +: keys: _*
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
    override val key = keys.head
  }
  
  case class SInter[R: Reader, CC[X] <: Traversable[X]](keys: String*)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SInter, keys: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
    override val key = keys.head
  }
  
  case class SInterStore(destination: String, keys: String*) extends Request[Long](
    SInterStore, destination +: keys: _*
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
    override val key = keys.head
  }
  
  case class SIsMember[W: Writer](key: String, member: W) extends Request[Boolean](
    SIsMember, key, implicitly[Writer[W]].write(member)
  ) with Key {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SMembers[R: Reader, CC[X] <: Traversable[X]](key: String)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SMembers, key) with Key {
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
  
  case class SPop[R: Reader](key: String) extends Request[Option[R]](SPop, key) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMember[R: Reader](key: String) extends Request[Option[R]](SRandMember, key) with Key {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMembers[R: Reader, CC[X] <: Traversable[X]](key: String, count: Int)(
    implicit cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SRandMember, key, count) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SRem[W](key: String, members: W*)(implicit writer: Writer[W]) extends Request[Long](
    SRem, key +: members.map(writer.write): _*
  ) with Key {
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
  ) with Key {
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
  ) extends Request[CC[R]](SUnion, keys: _*) with Key {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
    override val key = keys.head
  }
  
  case class SUnionStore(destination: String, keys: String*) extends Request[Long](
    SUnionStore, destination +: keys: _*
  ) with Key {
    override def decode = {
      case IntegerResponse(value) => value
    }
    override val key = keys.head
  }
  
}