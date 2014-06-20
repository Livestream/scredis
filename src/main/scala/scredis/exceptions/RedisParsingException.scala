package scredis.exceptions

/**
 * Exception thrown when the input could not be parsed by the provided [[scredis.parsing.Parser]].
 */
final case class RedisParsingException(cause: Throwable) extends RedisException(cause = cause)