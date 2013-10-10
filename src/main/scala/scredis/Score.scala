/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
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