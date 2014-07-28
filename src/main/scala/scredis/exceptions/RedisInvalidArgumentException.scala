package scredis.exceptions

/**
 * Exception thrown when the provided arguments of a command are invalid
 */
final case class RedisInvalidArgumentException(message: String) extends RedisException(message)