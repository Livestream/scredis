package scredis.commands

import scredis.{ClusterNode, ClusterSlotRange}
import scredis.io.ClusterConnection
import scredis.protocol.requests.ClusterRequests._

import scala.concurrent.Future

/**
 * Implements cluster commands.
 */
trait ClusterCommands { self: ClusterConnection =>

  /**
   * Assign new hash slots to receiving node
   *
   * @param slot a slot
   * @param slots more slots
   *
   * @since 3.0.0
   */
  def clusterAddSlots(slot: Long, slots: Long*): Future[Unit] = send(ClusterAddSlots((slot +: slots):_*))

  /**
   * Return the number of failure reports active for a given node.
   *
   * @param nodeId node to get failures for
   * @return the number of active failure reports for the node
   *
   * @since 3.0.0
   */
  def clusterCountFailureReports(nodeId: String): Future[Long] = send(ClusterCountFailureReports(nodeId))

  /**
   * Return the number of local keys in the specified hash slot.
   *
   * @param slot slot to count keys in
   * @return the number of local keys in the specified hash slot
   *
   * @since 3.0.0
   */
  def clusterCountKeysInSlot(slot: Long): Future[Long] = send(ClusterCountKeysInSlot(slot))

  /**
   * Set hash slots as unbound in receiving node.
   *
   * @param slot a slot
   * @param slots more slots
   * @return
   *
   * @since 3.0.0
   */
  def clusterDelSlots(slot: Long, slots: Long*): Future[Unit] = send(ClusterDelSlots((slot +: slots):_*))

  /**
   * Forces a slave to perform a manual failover of its master.
   */
  def clusterFailover(): Future[Unit] = send(ClusterFailover())

  /**
   * Forces a slave to perform a manual failover of its master.
   *
   * FORCE option: manual failover when the master is down.
   *
   * @since 3.0.0
   */
  def clusterFailoverForce(): Future[Unit] = send(ClusterFailoverForce())

  /**
   * Forces a slave to perform a manual failover of its master.
   *
   * TAKEOVER option: manual failover without cluster consensus.
   * TAKEOVER option implies everything FORCE implies, but also does not uses any cluster authorization
   * in order to failover.
   *
   * @since 3.0.0
   */
  def clusterFailoverTakeover(): Future[Unit] = send(ClusterFailoverTakeover())

  /**
   * Remove a node from the nodes table.
   *
   * @param nodeId node to remove
   *
   * @since 3.0.0
   */
  def clusterForget(nodeId: String): Future[Unit] = send(ClusterForget(nodeId))

  /**
   * Return local key names in the specified hash slot.
   *
   * @param slot slot to get key names from
   * @param count number of keys to return
   * @return local key names in the specified hash slot
   *
   * @since 3.0.0
   */
  def clusterGetKeysInSlot(slot: Long, count: Long): Future[Set[String]] = send(ClusterGetKeysInSlot[Set](slot, count))

  /**
   * Provides info about Redis Cluster node state.
   *
   * @return Key-value mapping of Redis Cluster vital parameters.
   *
   * @since 3.0.0
   */
  def clusterInfo(): Future[Map[String, String]] = send(ClusterInfo())

  /**
   * Returns the hash slot of the specified key.
   *
   * @param key
   * @return
   *
   * @since 3.0.0
   */
  def clusterKeyslot(key: String): Future[Long] = send(ClusterKeyslot(key))

  /**
   * Force a node cluster to handshake with another node.
   *
   * @param ip ip address of
   * @param port
   *
   * @since 3.0.0
   */
  def clusterMeet(ip: String, port: Long): Future[Unit] = send(ClusterMeet(ip, port))

  /**
   * Get Cluster config for the node.
   *
   * @return
   *
   * @since 3.0.0
   */
  def clusterNodes(): Future[Seq[ClusterNode]] = send(ClusterNodes())

  /**
   * Reconfigure a node as a slave of the specified master node.
   *
   * @param nodeId master node to replicate
   *
   * @since 3.0.0
   */
  def clusterReplicate(nodeId: String): Future[Unit] = send(ClusterReplicate(nodeId))

  /**
   * Reset a Redis Cluster node.
   *
   * @since 3.0.0
   */
  def clusterReset(): Future[Unit] = send(ClusterReset())

  /**
   * Reset a Redis Cluster node with HARD option.
   *
   * @since 3.0.0
   */
  def clusterResetHard(): Future[Unit] = send(ClusterResetHard())

  /**
   * Forces the node to save cluster state on disk.
   *
   * @since 3.0.0
   */
  def clusterSaveConfig(): Future[Unit] = send(ClusterSaveConfig())

  /**
   * Set the configuration epoch in a new node.
   *
   * @param configEpoch the config epoch to set
   *
   * @since 3.0.0
   */
  def clusterSetConfigEpoch(configEpoch: Long): Future[Unit] = send(ClusterSetConfigEpoch(configEpoch))

  /**
   * Set a hash slot in migrating state.
   *
   * @param slot slot to migrate
   * @param destinationNode node to migrate to
   *
   * @since 3.0.0
   */
  def clusterSetSlotMigrating(slot: Long, destinationNode: String): Future[Unit] = send(ClusterSetSlotMigrating(slot, destinationNode))

  /**
   * Set a hash slot in importing state.
   *
   * @param slot slot to import
   * @param sourceNode node to import from
   *
   * @since 3.0.0
   */
  def clusterSetSlotImporting(slot: Long, sourceNode: String): Future[Unit] = send(ClusterSetSlotImporting(slot, sourceNode))

  /**
   * Bind the hash slot to a different node.
   *
   * @param slot slot to associate with node
   * @param nodeId node to be associated with slot
   *
   * @since 3.0.0
   */
  def clusterSetSlotNode(slot: Long, nodeId: String): Future[Unit] = send(ClusterSetSlotNode(slot, nodeId))

  /**
   * Clear any importing / migrating state from hash slot.
   *
   * @param slot slot to clear of migrating/importing state
   *
   * @since 3.0.0
   */
  def clusterSetSlotStable(slot: Long): Future[Unit] = send(ClusterSetSlotStable(slot))

  /**
   * List slave nodes of the specified master node.
   *
   * @param nodeId node to list slave nodes of
   * @return slave nodes of the given master
   *
   * @since 3.0.0
   */
  def clusterSlaves(nodeId: String): Future[Seq[ClusterNode]] = send(ClusterSlaves(nodeId))

  /**
   * Get array of Cluster slot to node mappings.
   *
   * @return List of cluster slot ranges, with respective master and slave nodes.
   *
   * @since 3.0.0
   */
  def clusterSlots(): Future[List[ClusterSlotRange]] = send(ClusterSlots())

}
