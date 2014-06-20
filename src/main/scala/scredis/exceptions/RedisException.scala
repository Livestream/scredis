package scredis.exceptions

/**
 * Base class of all exceptions thrown by scredis
 */
abstract class RedisException(
  message: String = null,
  cause: Throwable = null
) extends Exception(message, cause)