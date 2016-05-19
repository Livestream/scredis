package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{Reader, Writer}

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
  
  case class SAdd[K, W](key: K, members: W*)(implicit writer: Writer[W], keyWriter: Writer[K]) extends Request[Long](
    SAdd, keyWriter.write(key) +: members.map(writer.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SCard[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Long](
    SCard, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SDiff[K, R: Reader, CC[X] <: Traversable[X]](keys: K*)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    SDiff, keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SDiffStore[KD, K](destination: KD, keys: K*)(
    implicit destKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Long](
    SDiffStore, destKeyWriter.write(destination) +: keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SInter[K, R: Reader, CC[X] <: Traversable[X]](keys: K*)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    SInter, keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SInterStore[KD, K](destination: KD, keys: K*)(
    implicit destinationKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Long](
    SInterStore, destinationKeyWriter.write(destination) +: keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SIsMember[K, W: Writer](key: K, member: W)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    SIsMember, keyWriter.write(key), implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SMembers[K, R: Reader, CC[X] <: Traversable[X]](key: K)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    SMembers, keyWriter.write(key)
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SMove[KS, KD, W: Writer](
    source: KS, destination: KD, member: W
  )(
    implicit sourceKeyWriter: Writer[KS], destinationKeyWriter: Writer[KD]
  ) extends Request[Boolean](
    SMove, sourceKeyWriter.write(source), destinationKeyWriter.write(destination), implicitly[Writer[W]].write(member)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class SPop[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    SPop, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMember[K, R: Reader](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[R]](
    SRandMember, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[R]
    }
  }
  
  case class SRandMembers[K, R: Reader, CC[X] <: Traversable[X]](key: K, count: Int)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](
    SRandMember, keyWriter.write(key), count
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SRem[K, W](key: K, members: W*)(implicit keyWriter: Writer[K], writer: Writer[W]) extends Request[Long](
    SRem, keyWriter.write(key) +: members.map(writer.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SScan[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    cursor: Long,
    matchOpt: Option[String],
    countOpt: Option[Int]
  )(implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]) extends Request[(Long, CC[R])](
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
  
  case class SUnion[K, R: Reader, CC[X] <: Traversable[X]](keys: K*)(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, R, CC[R]]
  ) extends Request[CC[R]](SUnion, keys.map(keyWriter.write): _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[R, CC] {
        case b: BulkStringResponse => b.flattened[R]
      }
    }
  }
  
  case class SUnionStore[KD, K](destination: KD, keys: K*)(
    implicit destinationKeyWriter: Writer[KD], keyWriter: Writer[K]
  ) extends Request[Long](
    SUnionStore, destinationKeyWriter.write(destination) +: keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
}