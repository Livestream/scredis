package scredis.exceptions

/**
 * Exception thrown when part of the response could not be deserialized by the provided
 * [[scredis.serialization.Reader]].
 */
final case class RedisReaderException(cause: Throwable) extends RedisException(cause = cause)