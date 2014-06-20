package scredis.exceptions

/**
 * Exception thrown when the Redis server replies with an error message
 */
final case class RedisErrorResponseException(message: String) extends RedisException(message)