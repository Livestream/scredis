package scredis

import scredis.commands.{StringCommands, KeyCommands}
import scredis.io.{ClusterConnection,Server}

/**
 * Created by justin on 09.04.15.
 */
class RedisCluster private[scredis] (nodes: List[Server]) extends ClusterConnection(nodes)
with KeyCommands
with StringCommands {

}
