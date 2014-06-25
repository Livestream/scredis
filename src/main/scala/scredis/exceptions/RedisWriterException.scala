package scredis.exceptions

/**
 * Exception thrown when a command argument could not be serialized by the provided
 * [[scredis.serialization.Writer]].
 */
final case class RedisWriterException(cause: Throwable) extends RedisException(cause = cause)