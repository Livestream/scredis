package scredis.exceptions

/**
 * A generic exception signaling some problem with cluster state.
 */
final case class RedisClusterException(message:String) extends RedisException(message) {

}
