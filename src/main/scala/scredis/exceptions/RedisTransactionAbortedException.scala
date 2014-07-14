package scredis.exceptions

/**
 * Exception resulting from a transaction being aborted due to watched key(s).
 */
final case object RedisTransactionAbortedException extends RedisException(
  "Transaction aborted due to watched key(s)"
)