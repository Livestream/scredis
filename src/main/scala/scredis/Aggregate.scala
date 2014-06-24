package scredis

/**
 * Represents the aggregation function to be used for aggregating scores when computing the
 * union or intersection of sorted sets
 */
sealed abstract class Aggregate(name: String) {
  override def toString = name
}

/**
 * Contains all available aggregation functions
 */
object Aggregate {
  case object Sum extends Aggregate("SUM")
  case object Min extends Aggregate("MIN")
  case object Max extends Aggregate("MAX")
}