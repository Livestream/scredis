package scredis.exceptions

import scredis.protocol.ClusterError

/**
 * Exception with a specific cluster error response, which allows a cluster client to respond by
 * redirecting the request and changing its state.
 */
final case class RedisClusterException(err: ClusterError)
