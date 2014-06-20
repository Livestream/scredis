package scredis.exceptions

/**
 * Wraps all IO exceptions
 */
final case class RedisIOException(
  message: String = null,
  cause: Throwable
) extends RedisException(message, cause)