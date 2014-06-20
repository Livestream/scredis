package scredis.exceptions

/**
 * Exception resulting from an unexpected breach of protocol such as receiving an unexpected reply
 * from the `Redis` server. This should never happen in practice.
 */
final case class RedisProtocolException(
  message: String,
  cause: Throwable = null
) extends RedisException(message, cause)