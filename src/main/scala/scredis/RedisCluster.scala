package scredis

import scredis.commands._
import scredis.io.{Connection, ClusterConnection}

import scala.concurrent.ExecutionContext


private[scredis] class RedisCluster(nodes: Seq[Server])
  extends ClusterConnection(nodes) with Connection
  with ClusterCommands
  with HashCommands
  with HyperLogLogCommands
  with KeyCommands
  with ListCommands
  with PubSubCommands
  with ScriptingCommands
  with SetCommands
  with SortedSetCommands
  with StringCommands
  //with SubscriberCommands
{
  override implicit val dispatcher: ExecutionContext =
    ExecutionContext.Implicits.global // TODO perhaps implement our own
}

object RedisCluster {
  // TODO all those other parameters, as applicable
  def apply(node: Server, nodes: Server*): RedisCluster = new RedisCluster(node +: nodes)
}