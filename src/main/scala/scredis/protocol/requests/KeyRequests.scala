package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration

object KeyRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.bytesReader
  
  object Del extends Command("DEL") with WriteCommand
  object Dump extends Command("DUMP")
  object Exists extends Command("EXISTS")
  object Expire extends Command("EXPIRE") with WriteCommand
  object ExpireAt extends Command("EXPIREAT") with WriteCommand
  object Keys extends Command("KEYS")
  object Migrate extends Command("MIGRATE") with WriteCommand
  object Move extends Command("MOVE") with WriteCommand
  object ObjectRefCount extends Command("OBJECT", "REFCOUNT")
  object ObjectEncoding extends Command("OBJECT", "ENCODING")
  object ObjectIdleTime extends Command("OBJECT", "IDLETIME")
  object Persist extends Command("PERSIST") with WriteCommand
  object PExpire extends Command("PEXPIRE") with WriteCommand
  object PExpireAt extends Command("PEXPIREAT") with WriteCommand
  object PTTL extends Command("PTTL")
  object RandomKey extends ZeroArgCommand("RANDOMKEY")
  object Rename extends Command("RENAME") with WriteCommand
  object RenameNX extends Command("RENAMENX") with WriteCommand
  object Restore extends Command("RESTORE") with WriteCommand
  object Scan extends Command("SCAN")
  object Sort extends Command("SORT")
  object TTL extends Command("TTL")
  object Type extends Command("TYPE")
  
  protected def generateSortArgs[K, KS](
    key: K,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean,
    storeKeyOpt: Option[KS]
  )(implicit keyWriter: Writer[K], storeKeyWriter: Writer[KS]): List[Any] = {
    val args = ListBuffer[Any]()
    args += keyWriter.write(key)
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
    storeKeyOpt.foreach { key =>
      args += "STORE" += storeKeyWriter.write(key)
    }
    args.toList
  }
  
  case class Del[K](keys: K*)(implicit keyWriter: Writer[K]) extends Request[Long](
    Del, keys.map(keyWriter.write): _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Dump[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[Array[Byte]]](
    Dump, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[Array[Byte]]
    }
  }
  
  case class Exists[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    Exists, keyWriter.write(key)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Expire[K](key: K, ttlSeconds: Int)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    Expire, keyWriter.write(key), ttlSeconds
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class ExpireAt[K](key: K, timestampSeconds: Long)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    ExpireAt, keyWriter.write(key), timestampSeconds
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Keys[CC[X] <: Traversable[X]](pattern: String)(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
  ) extends Request[CC[String]](Keys, pattern) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class Migrate[K](
    key: K,
    host: String,
    port: Int,
    database: Int,
    timeout: FiniteDuration,
    copy: Boolean,
    replace: Boolean
  )(implicit keyWriter: Writer[K]) extends Request[Unit](
    Migrate,
    {
      val args = ListBuffer[Any](host, port, keyWriter.write(key), database, timeout.toMillis)
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
  
  case class Move[K](key: K, database: Int)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    Move, keyWriter.write(key), database
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class ObjectRefCount[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[Long]](
    ObjectRefCount, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class ObjectEncoding[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[String]](
    ObjectEncoding, keyWriter.write(key)
  ) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String]
    }
  }
  
  case class ObjectIdleTime[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[Long]](
    ObjectIdleTime, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(value)   => Some(value)
      case BulkStringResponse(None) => None
    }
  }
  
  case class Persist[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    Persist, keyWriter.write(key)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PExpire[K](key: K, ttlMillis: Long)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    PExpire, keyWriter.write(key), ttlMillis
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PExpireAt[K](key: K, timestampMillis: Long)(implicit keyWriter: Writer[K]) extends Request[Boolean](
    PExpireAt, keyWriter.write(key), timestampMillis
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class PTTL[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Either[Boolean, Long]](
    PTTL, keyWriter.write(key)
  ) {
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
  
  case class Rename[K, KN](key: K, newKey: KN)(
    implicit keyWriter: Writer[K], newKeyWriter: Writer[KN]
  ) extends Request[Unit](
    Rename, keyWriter.write(key), newKeyWriter.write(newKey)
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class RenameNX[K, KN](key: K, newKey: KN)(
    implicit keyWriter: Writer[K], newKeyWriter: Writer[KN]
  )  extends Request[Boolean](
    RenameNX, keyWriter.write(key), newKeyWriter.write(newKey)
  ) {
    override def decode = {
      case i: IntegerResponse => i.toBoolean
    }
  }
  
  case class Restore[K, W: Writer](
    key: K, value: W, ttlOpt: Option[FiniteDuration]
  )(implicit keyWriter: Writer[K]) extends Request[Unit](
    Restore, keyWriter.write(key), ttlOpt.map(_.toMillis).getOrElse(0), implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case s: SimpleStringResponse => ()
    }
  }
  
  case class Scan[CC[X] <: Traversable[X]](
    cursor: Long, matchOpt: Option[String], countOpt: Option[Int]
  )(implicit cbf: CanBuildFrom[Nothing, String, CC[String]]) extends Request[(Long, CC[String])](
    Scan,
    generateScanLikeArgs[Boolean](
      keyOpt = None,
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    ): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsScanResponse[String, CC] {
        case a: ArrayResponse => a.parsed[String, CC] {
          case b: BulkStringResponse => b.flattened[String]
        }
      }
    }
  }
  
  case class Sort[K, R: Reader, CC[X] <: Traversable[X]](
    key: K,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean
  )(
    implicit keyWriter: Writer[K], cbf: CanBuildFrom[Nothing, Option[R], CC[Option[R]]]
  ) extends Request[CC[Option[R]]](
    Sort,
    generateSortArgs(key, byOpt, limitOpt, get, desc, alpha, None): _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[Option[R], CC] {
        case b: BulkStringResponse => b.parsed[R]
      }
    }
  }
  
  case class SortAndStore[K, KT](
    key: K,
    targetKey: KT,
    byOpt: Option[String],
    limitOpt: Option[(Long, Long)],
    get: Traversable[String],
    desc: Boolean,
    alpha: Boolean
  )(implicit keyWriter: Writer[K], targetKeyWriter: Writer[KT]) extends Request[Long](
    Sort,
    generateSortArgs(key, byOpt, limitOpt, get, desc, alpha, Some(targetKey)): _*
  ) {
    
    override def isReadOnly = false
    
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class TTL[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Either[Boolean, Int]](
    TTL, keyWriter.write(key)
  ) {
    override def decode = {
      case IntegerResponse(-2)  => Left(false)
      case IntegerResponse(-1)  => Left(true)
      case IntegerResponse(x)   => Right(x.toInt)
    }
  }
  
  case class Type[K](key: K)(implicit keyWriter: Writer[K]) extends Request[Option[scredis.Type]](
    Type, keyWriter.write(key)
  ) {
    override def decode = {
      case SimpleStringResponse("none") => None
      case SimpleStringResponse(value)  => Some(scredis.Type(value))
    }
  }

}