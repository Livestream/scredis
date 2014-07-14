package scredis.exceptions

/**
 * Exception that can be thrown while building a Transaction
 */
final case class RedisTransactionBuilderException(
  message: String = null, cause: Throwable = null
) extends RedisException(message, cause)