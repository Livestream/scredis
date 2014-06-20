package scredis.exceptions

/**
 * Exception resulting from a transaction being discarded.
 */
final case class RedisTransactionDiscardedException(reason: String) extends RedisException(reason)