package scredis

/**
 * Represents a condition that can be passed to some Redis command, e.g. SET
 */
sealed abstract class Condition(name: String) {
  override def toString = name
}

/**
 * Contains all available conditions
 */
object Condition {
  /**
   * Execute a command only if the value does not exist yet
   */
  case object IfDoesNotExist extends Condition("NX")
  /**
   * Execute a command only if the value already exists
   */
  case object IfAlreadyExists extends Condition("XX")
}