package object scredis {
  private var poolNumber = 0
  
  private[scredis] def newPoolNumber: Int = synchronized {
    poolNumber += 1
    poolNumber
  }
  
  val DefaultCommandOptions = CommandOptions()
  
  /**
   * Represents the type of a `Redis` key
   */
  sealed abstract class Type(val name: String) {
    override def toString = name
  }
  
  /**
   * Contains all available `Redis` types, i.e. string, list, set, zset and hash
   */
  object Type {
    case object String extends Type("string")
    case object List extends Type("list")
    case object Set extends Type("set")
    case object SortedSet extends Type("zset")
    case object Hash extends Type("hash")
    
    def apply(name: String): Type = name match {
      case String.name    => String
      case List.name      => List
      case Set.name       => Set
      case SortedSet.name => SortedSet
      case Hash.name      => Hash
      case x              => throw new IllegalArgumentException(s"Unknown type: $x")
    }
  }
  
  /**
   * Base class of a BITOP operation
   */
  sealed abstract class BitOp(val name: String) {
    override def toString = name
  }
  
  /**
   * Contains all available BITOP operations, i.e. AND, OR, XOR and NOT
   */
  object BitOp {
    case object And extends BitOp("AND")
    case object Or extends BitOp("OR")
    case object Xor extends BitOp("XOR")
    case object Not extends BitOp("NOT")
  }
  
  /**
   * Base class of a position used for the LINSERT command
   */
  sealed abstract class Position(val name: String) {
    override def toString = name
  }
  
  /**
   * Contains all available positions, i.e. BEFORE and AFTER
   */
  object Position {
    case object Before extends BitOp("BEFORE")
    case object After extends BitOp("AFTER")
  }
  
  /**
   * Represents the aggregation function to be used for aggregating scores when computing the
   * union or intersection of sorted sets
   */
  sealed abstract class Aggregate(val name: String) {
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
  
  /**
   * Represents the lower or upper bound of a range to be used when querying sorted sets by scores
   */
  sealed trait Score {
    
    /**
     * Returns the value of the score as a lower bound
     * @return lower bound score
     */
    def asMin: String
    
    /**
     * Returns the value of the score as an upper bound
     * @return upper bound score
     */
    def asMax: String
    
  }
  
  private[scredis] final class ScoreImpl private[scredis] (
    value: Option[Double], inclusive: Boolean
  ) extends Score {
    
    private val stringValueOpt: Option[String] = value.map { v =>
      if (inclusive) {
        v.toString
      } else {
        "(" + v
      }
    }
  
    def asMin: String = stringValueOpt.getOrElse("-inf")
    def asMax: String = stringValueOpt.getOrElse("+inf")
    
  }
  
  object Score {
    val Infinity = new ScoreImpl(None, true)
    
    /**
     * Returns an inclusive bound to be used as part of a range while querying a sorted set by scores
     * @param value the score bound's value
     * @return an inclusive score bound with provided value
     */
    def inclusive(value: Double): Score = new ScoreImpl(Some(value), true)
    
    /**
     * Returns an exclusive bound to be used as part of a range while querying a sorted set by scores
     * @param value the score bound's value
     * @return an exclusive score bound with provided value
     */
    def exclusive(value: Double): Score = new ScoreImpl(Some(value), false)
    
  }
  
  /**
   * Represents a condition that can be passed to some Redis command, e.g. SET
   */
  sealed abstract class Condition(val name: String) {
    override def toString = name
  }
  
  /**
   * Contains all available conditions
   */
  object Condition {
    /**
     * Execute a command only if the value does not exist yet
     */
    case object NX extends Condition("NX")
    /**
     * Execute a command only if the value already exists
     */
    case object XX extends Condition("XX")
  }
  
}