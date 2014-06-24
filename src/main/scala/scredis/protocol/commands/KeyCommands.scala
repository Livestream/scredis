package scredis.protocol.commands

import scredis.protocol._
import scredis.parsing.Parser

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object KeyCommands {
  
  import scredis.parsing.Implicits.stringParser
  
  object Del extends Command("DEL")
  object Dump extends Command("DUMP")
  object Exists extends Command("EXISTS")
  object Expire extends Command("EXPIRE")
  object ExpireAt extends Command("EXPIREAT")
  object Keys extends Command("KEYS")
  object Migrate extends Command("MIGRATE")
  object Move extends Command("MOVE")
  object Object extends Command("OBJECT")
  object Persist extends Command("PERSIST")
  object PExpire extends Command("PEXPIRE")
  object PExpireAt extends Command("PEXPIREAT")
  object PTTL extends Command("PTTL")
  object RandomKey extends ZeroArgCommand("RANDOMKEY")
  object Rename extends Command("RENAME")
  object RenameNX extends Command("RENAMENX")
  object Restore extends Command("RESTORE")
  object Sort extends Command("SORT")
  object TTL extends Command("TTL")
  object Type extends Command("TYPE")
  object Scan extends Command("SCAN")
  
  protected def generateSortArgs(
    key: String,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean,
    storeKeyOpt: Option[String]
  ): List[Any] = {
    val args = ListBuffer[Any]()
    args += key
    byOpt.foreach {
      args += "BY" += _
    }
    limitOpt.foreach {
      case (offset, limit) => args += "LIMIT" += offset += limit
    }
    get.foreach {
      args += "GET" += _
    }
    if (desc) {
      args += "DESC"
    }
    if (alpha) {
      args += "ALPHA"
    }
    storeKeyOpt.foreach {
      args += "STORE" += _
    }
    args.toList
  }
  
  case class Del(keys: String*) extends Request[Long](Del, keys: _*) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Dump(key: String) extends Request[Option[Array[Byte]]](Dump, key) {
    override def decode = {
      case BulkStringResponse(valueOpt) => valueOpt
    }
  }
  
  case class Exists(key: String) extends Request[Boolean](Exists, key) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Expire(key: String, ttlSeconds: Int) extends Request[Boolean](
    Expire, key, ttlSeconds
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class ExpireAt(key: String, timestampSeconds: Long) extends Request[Boolean](
    ExpireAt, key, timestampSeconds
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Keys(pattern: String) extends Request[Set[String]](Keys, pattern) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, Set] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class Migrate(
    key: String,
    host: String,
    port: Int,
    database: Int,
    timeout: FiniteDuration,
    copy: Boolean,
    replace: Boolean
  ) extends Request[Unit](
    Migrate,
    {
      val args = ListBuffer[Any](host, port, key, database, timeout.toMillis)
      if (copy) {
        args += "COPY"
      }
      if (replace) {
        args += "REPLACE"
      }
      args.toList
    }: _*
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Move(key: String, database: Int) extends Request[Boolean](Move, key, database) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Persist(key: String) extends Request[Boolean](Persist, key) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PExpire(key: String, ttlMillis: Long) extends Request[Boolean](
    PExpire, key, ttlMillis
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PExpireAt(key: String, timestampMillis: Long) extends Request[Boolean](
    PExpireAt, key, timestampMillis
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PTTL(key: String) extends Request[Either[Boolean, Long]](PTTL, key) {
    override def decode = {
      case IntegerResponse(-2)  => Left(false)
      case IntegerResponse(-1)  => Left(true)
      case IntegerResponse(x)   => Right(x)
    }
  }
  
  case class RandomKey() extends Request[Option[String]](RandomKey) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String]
    }
  }
  
  case class Rename(key: String, newKey: String) extends Request[Unit](Rename, key, newKey) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class RenameNX(key: String, newKey: String) extends Request[Boolean](Rename, key, newKey) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Restore(
    key: String, value: Array[Byte], ttlOpt: Option[FiniteDuration]
  ) extends Request[Unit](Restore, ttlOpt.map(_.toMillis).getOrElse(0), value) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Scan(
    cursor: Long, matchOpt: Option[String], countOpt: Option[Int]
  ) extends Request[(Long, Set[String])](
    Scan,
    generateScanLikeArgs(
      keyOpt = None,
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    ): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[String, Set] {
        case a: ArrayResponse => a.parsed[String, Set] {
          case b: BulkStringResponse => b.flattened[String]
        }
      }
    }
  }
  
  case class Sort[A, CC[X] <: Traversable[X]](
    key: String,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean
  )(
    implicit parser: Parser[A],
    cbf: CanBuildFrom[Nothing, Option[A], CC[Option[A]]]
  ) extends Request[CC[Option[A]]](
    Sort,
    generateSortArgs(key, byOpt, limitOpt, get, desc, alpha, None): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[A], CC] {
        case b: BulkStringResponse => b.parsed[A]
      }
    }
  }
  
  case class SortAndStore(
    key: String,
    targetKey: String,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean
  ) extends Request[Long](
    Sort,
    generateSortArgs(key, byOpt, limitOpt, get, desc, alpha, Some(targetKey)): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class TTL(key: String) extends Request[Either[Boolean, Int]](TTL, key) {
    override def decode = {
      case IntegerResponse(-2)  => Left(false)
      case IntegerResponse(-1)  => Left(true)
      case IntegerResponse(x)   => Right(x.toInt)
    }
  }
  
  case class Type(key: String) extends Request[Option[scredis.Type]](Type, key) {
    override def decode = {
      case SimpleStringResponse("none") => None
      case SimpleStringResponse(value)  => Some(scredis.Type(value))
    }
  }

}