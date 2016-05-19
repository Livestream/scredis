package scredis.protocol.requests

import scala.language.postfixOps
import scala.language.higherKinds
import scredis.protocol.{ Command => PCommand, _ }
import scredis.exceptions.RedisProtocolException
import scredis.serialization.Writer

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

object ServerRequests {
  
  import scredis.serialization.Implicits.stringReader
  import scredis.serialization.Implicits.longReader
  
  private val commandInfoMapDecoder: Decoder[Map[String, scredis.CommandInfo]] = {
    case a: ArrayResponse => a.parsed[Option[(String, scredis.CommandInfo)], List] {
      case a: ArrayResponse => {
        val info = a.parsed[Any, Vector] {
          case b: BulkStringResponse => b
          case IntegerResponse(value) => value.toInt
          case a: ArrayResponse => a.parsed[String, scala.collection.immutable.HashSet] {
            case SimpleStringResponse(value) => value
            case x => throw new IllegalArgumentException(s"UNEXPECTED response for FLAGS: $x")
          }
          case x => throw new IllegalArgumentException(s"UNEXPECTED response for INFO: $x")
        }
        val name = info(0).asInstanceOf[BulkStringResponse].flattened[String]
        val arity = info(1).asInstanceOf[Int]
        val flags = info(2).asInstanceOf[scala.collection.immutable.HashSet[String]]
        val firstKeyPosition = info(3).asInstanceOf[Int]
        val lastKeyPosition = info(4).asInstanceOf[Int]
        val keyStepCount = info(5).asInstanceOf[Int]
        Some(
          (
            name,
            scredis.CommandInfo(
              name = name,
              arity = arity,
              flags = scredis.CommandFlags(flags),
              firstKeyPosition = firstKeyPosition,
              lastKeyPosition = lastKeyPosition,
              keyStepCount = keyStepCount
            )
          )
        )
      }
      case BulkStringResponse(None) => None
      case x => throw new IllegalArgumentException(s"UNEXPECTED response for LIST: $x")
    }.flatten.toMap
    case x => throw new IllegalArgumentException(s"UNEXPECTED response for ROOT: $x")
  }
  
  object BGRewriteAOF extends ZeroArgCommand("BGREWRITEAOF")
  object BGSave extends ZeroArgCommand("BGSAVE")
  object ClientGetName extends ZeroArgCommand("CLIENT", "GETNAME")
  object ClientKill extends PCommand("CLIENT", "KILL")
  object ClientList extends ZeroArgCommand("CLIENT", "LIST")
  object ClientPause extends PCommand("CLIENT", "PAUSE")
  object ClientSetName extends PCommand("CLIENT", "SETNAME")
  object Command extends ZeroArgCommand("COMMAND")
  object CommandCount extends ZeroArgCommand("COMMAND", "COUNT")
  object CommandGetKeys extends PCommand("COMMAND", "GETKEYS")
  object CommandInfo extends PCommand("COMMAND", "INFO")
  object ConfigGet extends PCommand("CONFIG", "GET")
  object ConfigResetStat extends ZeroArgCommand("CONFIG", "RESETSTAT")
  object ConfigRewrite extends ZeroArgCommand("CONFIG", "REWRITE")
  object ConfigSet extends PCommand("CONFIG", "SET")
  object DBSize extends ZeroArgCommand("DBSIZE")
  object FlushAll extends ZeroArgCommand("FLUSHALL") with WriteCommand
  object FlushDB extends ZeroArgCommand("FLUSHDB") with WriteCommand
  object Info extends PCommand("INFO")
  object LastSave extends ZeroArgCommand("LASTSAVE")
  object Role extends ZeroArgCommand("ROLE")
  object Save extends ZeroArgCommand("SAVE") with WriteCommand
  object Shutdown extends PCommand("SHUTDOWN")
  object SlaveOf extends PCommand("SLAVEOF")
  object SlowLogGet extends PCommand("SLOWLOG", "GET")
  object SlowLogLen extends ZeroArgCommand("SLOWLOG", "LEN")
  object SlowLogReset extends ZeroArgCommand("SLOWLOG", "RESET")
  object Time extends ZeroArgCommand("TIME")
  
  case class BGRewriteAOF() extends Request[Unit](BGRewriteAOF) {
    override def decode = {
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class BGSave() extends Request[Unit](BGSave) {
    override def decode = {
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class ClientGetName() extends Request[Option[String]](ClientGetName) {
    override def decode = {
      case b: BulkStringResponse => b.parsed[String]
    }
  }
  
  case class ClientKill(ip: String, port: Int) extends Request[Unit](ClientKill, s"$ip:$port") {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  /*
   *  Note: Redis does not support killing multiple clients
   *  (e.g. KILL ID 1 ID 2 will only kill the last one)
   *  This is why arguments are Options and not Seqs
   */
  case class ClientKillWithFilters(
    addrOpt: Option[(String, Int)],
    idOpt: Option[Long],
    typeOpt: Option[scredis.ClientType],
    skipMe: Boolean
  ) extends Request[Long](
    ClientKill,
    {
      val args = ListBuffer[Any]()
      addrOpt.foreach {
        case (ip, port) => args += "ADDR" += s"$ip:$port"
      }
      idOpt.foreach {
        args += "ID" += _
      }
      typeOpt.foreach {
        args += "TYPE" += _
      }
      if (!skipMe) {
        args += "SKIPME" += "no"
      }
      args.toList
    }: _*
  ) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class ClientList() extends Request[List[Map[String, String]]](ClientList) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[String].split("\n").map { line =>
        line.split(" ").flatMap { keyValue =>
          val split = keyValue.split("=")
          if (split.size == 2) {
            val key = split(0).trim()
            val value = split(1).trim()
            Some((key, value))
          } else {
            None
          }
        }.toMap
      }.toList
    }
  }
  
  case class ClientPause(timeoutMillis: Long) extends Request[Unit](ClientPause, timeoutMillis) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class ClientSetName(name: String) extends Request[Unit](ClientSetName, name) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class Command() extends Request[Map[String, scredis.CommandInfo]](Command) {
    override def decode = commandInfoMapDecoder
  }
  
  case class CommandCount() extends Request[Int](CommandCount) {
    override def decode = {
      case IntegerResponse(value) => value.toInt
    }
  }
  
  case class CommandGetKeys[CC[X] <: Traversable[X]](command: String)(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
  ) extends Request[CC[String]](CommandGetKeys, command.split("\\s+"): _*) {
    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class CommandInfo(
    commands: String*
  ) extends Request[Map[String, scredis.CommandInfo]](CommandInfo, commands: _*) {
    override def decode = commandInfoMapDecoder
  }
  
  case class ConfigGet(parameter: String) extends Request[Map[String, String]](
    ConfigGet, parameter
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsedAsPairsMap[String, String, Map] {
        case b: BulkStringResponse => b.flattened[String]
      } {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }
  
  case class ConfigResetStat() extends Request[Unit](ConfigResetStat) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class ConfigRewrite() extends Request[Unit](ConfigRewrite) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class ConfigSet[W: Writer](parameter: String, value: W) extends Request[Unit](
    ConfigSet, parameter, implicitly[Writer[W]].write(value)
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class DBSize() extends Request[Long](DBSize) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class FlushAll() extends Request[Unit](FlushAll) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class FlushDB() extends Request[Unit](FlushDB) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class Info(sectionOpt: Option[String]) extends Request[Map[String, String]](
    Info, sectionOpt.toSeq: _*
  ) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[String].split("\r\n").flatMap { keyValue =>
        val split = keyValue.split(":")
        if (split.length > 1) {
          val key = split(0).trim()
          val value = split(1).trim()
          Some(key -> value)
        } else {
          None
        }
      }.toMap
    }
  }
  
  case class LastSave() extends Request[Long](LastSave) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class Role() extends Request[scredis.Role](Role) {
    override def decode = {
      case a: ArrayResponse => a.headOpt[String] {
        case b: BulkStringResponse => b.flattened[String]
      } match {
        case Some("master") => {
          val data = a.parsed[Any, IndexedSeq] {
            case b: BulkStringResponse => b.flattened[String]
            case IntegerResponse(value) => value
            case a: ArrayResponse => a.parsed[scredis.Role.SlaveInfo, List] {
              case a: ArrayResponse => {
                val slaveData = a.parsed[String, IndexedSeq] {
                  case b: BulkStringResponse => b.flattened[String]
                }
                scredis.Role.SlaveInfo(
                  ip = slaveData(0),
                  port = slaveData(1).toInt,
                  replicationOffset = slaveData(2).toLong
                )
              }
            }
          }
          scredis.Role.Master(
            replicationOffset = data(1).toString.toLong,
            connectedSlaves = data(2).asInstanceOf[List[scredis.Role.SlaveInfo]]
          )
        }
        case Some("slave") => {
          val data = a.parsed[Any, IndexedSeq] {
            case b: BulkStringResponse => b.flattened[String]
            case IntegerResponse(value) => value
          }
          scredis.Role.Slave(
            masterIp = data(1).toString,
            masterPort = data(2).toString.toInt,
            replicationState = scredis.Role.ReplicationState(data(3).toString),
            replicationOffset = data(4).toString.toLong
          )
        }
        case Some("sentinel") => {
          val data = a.parsed[Any, IndexedSeq] {
            case b: BulkStringResponse => b.flattened[String]
            case a: ArrayResponse => a.parsed[String, List] {
              case b: BulkStringResponse => b.flattened[String]
            }
          }
          scredis.Role.Sentinel(
            monitoredMasterNames = data(1).asInstanceOf[List[String]]
          )
        }
        case Some(x) => throw RedisProtocolException(s"Unexpected role: $x")
        case None => throw RedisProtocolException(s"Unexpected empty array for role")
      }
    }
  }
  
  case class Save() extends Request[Unit](Save) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class Shutdown(modifierOpt: Option[scredis.ShutdownModifier]) extends Request[Unit](
    Shutdown, modifierOpt.map(_.name).toSeq: _*
  ) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SlaveOf(host: String, port: Int) extends Request[Unit](SlaveOf, host, port) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SlaveOfNoOne() extends Request[Unit](SlaveOf, "NO", "ONE") {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class SlowLogGet[CC[X] <: Traversable[X]](countOpt: Option[Int])(
    implicit cbf: CanBuildFrom[Nothing, scredis.SlowLogEntry, CC[scredis.SlowLogEntry]]
  ) extends Request[CC[scredis.SlowLogEntry]](
    SlowLogGet, countOpt.toSeq: _*
  ) {
    override def decode = {
      case a: ArrayResponse => a.parsed[scredis.SlowLogEntry, CC] {
        case a: ArrayResponse => {
          val data = a.parsed[Any, IndexedSeq] {
            case IntegerResponse(value) => value
            case a: ArrayResponse => a.parsed[String, List] {
              case b: BulkStringResponse => b.flattened[String]
            }
          }
          scredis.SlowLogEntry(
            uid = data(0).toString.toLong,
            timestampSeconds = data(1).toString.toLong,
            executionTime = (data(2).toString.toLong microseconds),
            command = data(3).asInstanceOf[List[String]]
          )
        }
      }
    }
  }
  
  case class SlowLogLen() extends Request[Long](SlowLogLen) {
    override def decode = {
      case IntegerResponse(value) => value
    }
  }
  
  case class SlowLogReset() extends Request[Unit](SlowLogReset) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class Time() extends Request[(Long, Long)](Time) {
    override def decode = {
      case a: ArrayResponse => {
        val array = a.parsed[Long, IndexedSeq] {
          case b: BulkStringResponse => b.flattened[Long]
        }
        (array(0), array(1))
      }
    }
  }

}