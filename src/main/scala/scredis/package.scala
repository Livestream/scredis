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
   * Represents the score of sorted set member
   */
  abstract class Score(val stringValue: String) {
    override def toString: String = stringValue
  }
  
  /**
   * Contains all possible score values, i.e. -inf, +inf or value
   */
  object Score {
    case object MinusInfinity extends Score("-inf")
    case object PlusInfinity extends Score("+inf")
    case class Value(value: Double) extends Score(value.toString)
    
    def apply(stringValue: String): Score = stringValue.toLowerCase() match {
      case MinusInfinity.stringValue | "inf"  => MinusInfinity
      case PlusInfinity.stringValue           => PlusInfinity
      case x                                  => Value(x.toDouble)
    }
  }
  
  /**
   * Represents one end of an interval as defined by sorted set commands
   */
  abstract class ScoreLimit(val stringValue: String) {
    override def toString: String = stringValue
  }
  
  /**
   * Contains all possible score limit values, i.e. -inf, +inf, inclusive value and exclusive value
   */
  object ScoreLimit {
    case object MinusInfinity extends ScoreLimit("-inf")
    case object PlusInfinity extends ScoreLimit("+inf")
    case class Inclusive(value: Double) extends ScoreLimit(value.toString)
    case class Exclusive(value: Double) extends ScoreLimit("(" + value)
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