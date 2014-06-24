package scredis

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
    if (inclusive) v.toString else "(" + v
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