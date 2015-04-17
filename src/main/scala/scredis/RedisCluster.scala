package scredis

import scredis.commands._
import scredis.io.{Connection, ClusterConnection, Server}

import scala.concurrent.ExecutionContext

/**
 * Created by justin on 09.04.15.
 */
class RedisCluster private[scredis] (nodes: List[Server])
  extends ClusterConnection(nodes) with Connection
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
  def apply(nodes: List[Server]): RedisCluster = new RedisCluster(nodes)
}