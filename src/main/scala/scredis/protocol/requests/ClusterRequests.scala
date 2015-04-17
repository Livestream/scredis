package scredis.protocol.requests

import scredis.ClusterSlotRange
import scredis.protocol._

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

  case class ClusterAddSlots(slot: Long, slots: Long*) extends Request[Unit](ClusterAddSlots, (slot +: slots): _*) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterCountFailureReports(nodeId: String) extends Request[Long](ClusterCountFailureReports, nodeId) {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterCountKeysInSlot(slot: Long) extends Request[Long](ClusterCountKeysInSlot, slot) {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterDelSlots(slot: Long, slots: Long*) extends Request[Unit](ClusterDelSlots, (slot +: slots): _*) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailover() extends Request[Unit](ClusterFailover) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailoverForce() extends Request[Unit](ClusterFailover, "FORCE") {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterFailoverTakeover() extends Request[Unit](ClusterFailover, "TAKEOVER") {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterForget(node: String) extends Request[Unit](ClusterForget, node) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterGetKeysInSlot[CC[X] <: Traversable[X]](slot: Long, count: Long)(
    implicit cbf: CanBuildFrom[Nothing, String, CC[String]]
    ) extends Request[CC[String]](ClusterGetKeysInSlot, slot, count) {

    override def decode = {
      case a: ArrayResponse => a.parsed[String, CC] {
        case b: BulkStringResponse => b.flattened[String]
      }
    }
  }

  case class ClusterInfo() extends Request[Map[String, String]](ClusterInfo) {
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

  case class ClusterKeyslot(key: String) extends Request[Long](ClusterKeyslot,key) {
    override def decode: Decoder[Long] = {
      case IntegerResponse(value) => value
    }
  }

  case class ClusterMeet(ip: String, port: Int) extends Request[Unit](ClusterMeet, ip, port) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterNodes() extends Request[String](ClusterNodes) {
    override def decode: Decoder[String] = {
      // TODO decode the full structure of this reply
      case b: BulkStringResponse => b.flattened[String]
    }
  }

  case class ClusterReplicate(node: String) extends Request[Unit](ClusterReplicate, node) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterReset() extends Request[Unit](ClusterReset) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterResetSoft() extends Request[Unit](ClusterReset, "SOFT") {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterResetHard() extends Request[Unit](ClusterReset, "HARD") {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSaveConfig() extends Request[Unit](ClusterSaveConfig) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetConfigEpoch(configEpoch: Long) extends Request[Unit](ClusterSetConfigEpoch,configEpoch) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotMigrating(slot: Long, destinationNode: String) extends Request[Unit](ClusterSetSlot, slot, "MIGRATING", destinationNode) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotImporting(slot: Long, sourceNode: String) extends Request[Unit](ClusterSetSlot, slot, "IMPORTING", sourceNode) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }
  case class ClusterSetSlotNode(slot: Long, node: String) extends Request[Unit](ClusterSetSlot, slot, "NODE", node) {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSetSlotStable(slot: Long) extends Request[Unit](ClusterSetSlot, slot, "STABLE") {
    override def decode: Decoder[Unit] = {
      case SimpleStringResponse(_) => ()
    }
  }

  case class ClusterSlaves(node: String) extends Request[String](ClusterSlaves) {
    override def decode: Decoder[String] = {
      // TODO decode the full structure of this reply
      case b: BulkStringResponse => b.flattened[String]
    }
  }

  case class ClusterSlots() extends Request[List[ClusterSlotRange]](ClusterSlots) {
    override def decode: Decoder[List[ClusterSlotRange]] = {
      case a: ArrayResponse => a.parsedAsClusterSlotsResponse
    }
  }
}
