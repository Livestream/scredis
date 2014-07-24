package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.HyperLogLogRequests._
import scredis.serialization.Writer

import scala.concurrent.Future

/**
 * This trait implements hyperloglog commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait HyperLogLogCommands { self: NonBlockingConnection =>
  
  /**
   * Adds all the element arguments to the HyperLogLog data structure stored at specified key.
   *
   * @note The HyperLogLog data structure can be used in order to count unique elements in a set
   * using just a small constant amount of memory, specifically 12k bytes for every HyperLogLog
   * (plus a few bytes for the key itself).
   * 
   * The returned cardinality of the observed set is not exact, but approximated with a standard
   * error of 0.81%.
   * 
   * @param key the key of the HyperLogLog data structure
   * @param elements element(s) to add to the HyperLogLog data structure
   * @return $true if at least one HyperLogLog internal register was altered, $false otherwise
   * @throws $e if key is not of HyperLogLog type
   *
   * @since 2.8.9
   */
  def pfAdd[W: Writer](key: String, elements: W*): Future[Boolean] = send(
    PFAdd(key, elements: _*)
  )
  
  /**
   * When called with a single key, returns the approximated cardinality computed by the
   * HyperLogLog data structure stored at the specified variable, which is 0 if the variable
   * does not exist.
   * 
   * When called with multiple keys, returns the approximated cardinality of the union of the
   * HyperLogLogs passed, by internally merging the HyperLogLogs stored at the provided keys into
   * a temporary HyperLogLog.
   * 
   * @note The HyperLogLog data structure can be used in order to count unique elements in a set
   * using just a small constant amount of memory, specifically 12k bytes for every HyperLogLog
   * (plus a few bytes for the key itself).
   * 
   * The returned cardinality of the observed set is not exact, but approximated with a standard
   * error of 0.81%.
   *
   * @param keys key(s) of the HyperLogLog data structure to count
   * @return the approximate number of unique elements observed via PFADD
   * @throws $e if at least one key is not of HyperLogLog type
   *
   * @since 2.8.9
   */
  def pfCount(keys: String*): Future[Long] = send(
    PFCount(keys: _*)
  )
  
  /**
   * Merge multiple HyperLogLog values into an unique value that will approximate the cardinality
   * of the union of the observed Sets of the source HyperLogLog structures.
   * 
   * The computed merged HyperLogLog is set to the destination variable, which is created if does
   * not exist (defauling to an empty HyperLogLog).
   * 
   * @note The HyperLogLog data structure can be used in order to count unique elements in a set
   * using just a small constant amount of memory, specifically 12k bytes for every HyperLogLog
   * (plus a few bytes for the key itself).
   * 
   * The returned cardinality of the observed set is not exact, but approximated with a standard
   * error of 0.81%.
   *
   * @param destKey the destination key where the result should be stored
   * @param keys keys of the HyperLogLog data structure to merge
   * @throws $e if at least one key is not of HyperLogLog type
   *
   * @since 2.8.9
   */
  def pfMerge(destKey: String, keys: String*): Future[Unit] = send(
    PFMerge(destKey, keys: _*)
  )
  
}