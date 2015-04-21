package scredis.protocol.requests

import scredis.{Server, ClusterNode, ClusterSlotRange}
import scredis.protocol._
import scredis.serialization.Reader

import scala.collection.generic.CanBuildFrom

object ClusterRequests {

  object ClusterAddSlots extends Command("CLUSTER","ADDSLOTS")
  object ClusterCountFailureReports extends Command("CLUSTER","COUNT-FAILURE-REPORTS")
  object ClusterCountKeysInSlot extends Command("CLUSTER","COUNTKEYSINSLOT")
  object ClusterDelSlots extends Command("CLUSTER","DELSLOTS")
  object ClusterFailover extends Command("CLUSTER","FAILOVER")
  object ClusterForget extends Command("CLUSTER","FORGET")
  object ClusterGetKeysInSlot extends Command("CLUSTER","GETKEYSINSLOT")
  object ClusterInfo extends ZeroArgCommand("CLUSTER","INFO")
  object ClusterKeyslot extends Command("CLUSTER","KEYSLOT")
  object ClusterMeet extends Command("CLUSTER","MEET")
  object ClusterNodes extends ZeroArgCommand("CLUSTER","NODES")
  object ClusterReplicate extends Command("CLUSTER","REPLICATE")
  object ClusterReset extends Command("CLUSTER","RESET")
  object ClusterSaveConfig extends ZeroArgCommand("CLUSTER","SAVECONFIG")
  object ClusterSetConfigEpoch extends Command("CLUSTER","SET-CONFIG-EPOCH")
  object ClusterSetSlot extends Command("CLUSTER","SETSLOT")
  object ClusterSlaves extends Command("CLUSTER","SLAVES")
  object ClusterSlots extends ZeroArgCommand("CLUSTER","SLOTS")

  case class ClusterAddSlots(slots: Long*) extends Request[Unit](ClusterAddSlots, slots: _*) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterCountFailureReports(nodeId: String)
    extends Request[Long](ClusterCountFailureReports, nodeId) with Cluster {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterCountKeysInSlot(slot: Long)
    extends Request[Long](ClusterCountKeysInSlot, slot) with Cluster {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterDelSlots(slots: Long*)
    extends Request[Unit](ClusterDelSlots, slots: _*) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailover() extends Request[Unit](ClusterFailover) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailoverForce() extends Request[Unit](ClusterFailover, "FORCE") with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailoverTakeover() extends Request[Unit](ClusterFailover, "TAKEOVER") with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterForget(nodeId: String) extends Request[Unit](ClusterForget, nodeId) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterGetKeysInSlot[CC[X] <: Traversable[X]](slot: Long, count: Long)(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
    ) extends Request[CC[String]](ClusterGetKeysInSlot, slot, count) with Cluster {

    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case SimpleStringResponse(s) => s
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }

  case class ClusterInfo() extends Request[Map[String, String]](ClusterInfo) with Cluster {
    override def decode = { // copypaste from KeyRequests.Info :(
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

  case class ClusterKeyslot(key: String) extends Request[Long](ClusterKeyslot,key) with Cluster {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterMeet(ip: String, port: Long) extends Request[Unit](ClusterMeet, ip, port) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterNodes() extends Request[Seq[ClusterNode]](ClusterNodes) with Cluster {
    override def decode: Decoder[Seq[ClusterNode]] = {
      // TODO decode the full structure of this reply
      case b: BulkStringResponse =>
        parseClusterNodes(b.flattened[String])
    }
  }

  case class ClusterReplicate(nodeId: String) extends Request[Unit](ClusterReplicate, nodeId) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterReset() extends Request[Unit](ClusterReset) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterResetSoft() extends Request[Unit](ClusterReset, "SOFT") with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterResetHard() extends Request[Unit](ClusterReset, "HARD") with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSaveConfig() extends Request[Unit](ClusterSaveConfig) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetConfigEpoch(configEpoch: Long)
    extends Request[Unit](ClusterSetConfigEpoch,configEpoch) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotMigrating(slot: Long, destinationNode: String)
    extends Request[Unit](ClusterSetSlot, slot, "MIGRATING", destinationNode) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotImporting(slot: Long, sourceNode: String)
    extends Request[Unit](ClusterSetSlot, slot, "IMPORTING", sourceNode) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }
  case class ClusterSetSlotNode(slot: Long, nodeId: String)
    extends Request[Unit](ClusterSetSlot, slot, "NODE", nodeId) with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotStable(slot: Long) extends Request[Unit](ClusterSetSlot, slot, "STABLE") with Cluster {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSlaves(nodeId: String) extends Request[Seq[ClusterNode]](ClusterSlaves, nodeId) with Cluster {
    override def decode: Decoder[Seq[ClusterNode]] = {
      case a: ArrayResponse =>
        a.parsed[ClusterNode,Vector] {
          case b: BulkStringResponse => parseClusterNode(b.flattened)
        }
      case b: BulkStringResponse =>
        // documentation says response has same format as CLUSTER NODES, but it's actually array response here
        // let's just handle both cases, in case they change it
        parseClusterNodes(b.flattened[String])
    }
  }

  case class ClusterSlots() extends Request[List[ClusterSlotRange]](ClusterSlots) with Cluster {
    override def decode: Decoder[List[ClusterSlotRange]] = {
      case a: ArrayResponse =>
        a.parsedAsClusterSlotsResponse[List]
    }
  }

  def clusterNodeDecoder: Decoder[ClusterNode] = {
    case SimpleStringResponse(s) => parseClusterNode(s)
  }

  def parseClusterNode(line: String): ClusterNode = {
    val fields = line.split(' ')
    val id = fields(0)
    val Array(host,port) = fields(1).split(':')
    val flags = fields(2).split(',').toVector
    val master = if (fields(3) == "-") None else Some(fields(3))
    val pingSent = fields(4).toLong
    val pongRecv = fields(5).toLong
    val configEpoch = fields(6).toLong
    val linkStateConnected = fields(7) == "connected"
    val slots = fields.slice(8, fields.size).map { slot =>
      slot.split('-') match {
        case Array(s) => (s.toLong,s.toLong)
        case Array(begin,end) => (begin.toLong,end.toLong)
      }
    }.toVector

    ClusterNode(id,Server(host,port.toInt),flags,master,pingSent,pongRecv,configEpoch,linkStateConnected,slots)
  }

  def parseClusterNodes(raw: String): Seq[ClusterNode] = {
    // parse according to http://redis.io/commands/cluster-nodes#serialization-format
    val lines = raw.split('\n')
    lines.map { line => parseClusterNode(line) }.toVector
  }
}
