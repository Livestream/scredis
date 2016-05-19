package scredis.commands

import scredis.io.NonBlockingConnection
import scredis.protocol.requests.SetRequests._
import scredis.serialization.{ Reader, Writer }

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * This trait implements set commands.
 *
 * @define e [[scredis.exceptions.RedisErrorResponseException]]
 * @define none `None`
 * @define true '''true'''
 * @define false '''false'''
 */
trait SetCommands { self: NonBlockingConnection =>
  
  /**
   * Adds one or more members to a set.
   *
   * @param key set key
   * @param members member(s) to add
   * @return the number of members added to the set, not including all the members that were
   * already present
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sAdd[K: Writer, W: Writer](key: K, members: W*): Future[Long] = send(SAdd(key, members: _*))
  
  /**
   * Returns the number of members in a set.
   *
   * @param key set key
   * @return the cardinality (number of members) of the set, or 0 if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sCard[K: Writer](key: K): Future[Long] = send(SCard(key))
  
  /**
   * Returns the set resulting from the difference between the first set and all the successive
   * sets.
   * 
   * @param key the key of the first set
   * @param keys key(s) of successive set(s) whose members will be substracted from the first one
   * @return the resulting set, or the empty set if the first key does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiff[K: Writer, R: Reader](key: K, keys: K*): Future[Set[R]] = send(
    SDiff[K, R, Set](key +: keys: _*)
  )
  
  /**
   * Stores the set resulting from the difference between the first set and all the successive sets.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param key key of first set
   * @param keys keys of sets to be substracted from first set, if empty, first set is simply
   * copied to destKey
   * @return the cardinality of the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiffStore[KD: Writer, K: Writer](destKey: KD, key: K, keys: K*): Future[Long] = send(
    SDiffStore(destKey, key +: keys: _*)
  )
  
  /**
   * Intersects multiple sets.
   *
   * @param keys keys of sets to be intersected together
   * @return the resulting set, or the empty set if the first key does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInter[K: Writer, R: Reader](keys: K*): Future[Set[R]] = send(SInter[K, R, Set](keys: _*))
  
  /**
   * Intersects multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param keys keys of sets to be intersected together, if only one is specified, it is simply
   * copied to destKey
   * @return the cardinality of the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInterStore[KD: Writer, K: Writer](destKey: KD, keys: K*): Future[Long] = send(
    SInterStore(destKey, keys: _*)
  )
  
  /**
   * Determines if a given value is a member of a set.
   *
   * @param key set key
   * @param member value to be tested
   * @return $true if the provided value is a member of the set stored at key, $false otherwise
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sIsMember[K: Writer, W: Writer](key: K, member: W): Future[Boolean] = send(
    SIsMember(key, member)
  )
  
  /**
   * Returns all the members of a set.
   *
   * @param key set key
   * @return set stored at key, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMembers[K: Writer, R: Reader](key: K): Future[Set[R]] = send(SMembers[K, R, Set](key))
  
  /**
   * Moves a member from one set to another.
   *
   * @param sourceKey key of source set
   * @param deskKey key of destination set
   * @param member value to be moved from source set to destination set
   * @return $true if the member was moved, $false if the element is not a member of source set and
   * no operation was performed
   * @throws $e if sourceKey or destKey contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMove[KS: Writer, KD: Writer, W: Writer](sourceKey: KS, destKey: KD, member: W): Future[Boolean] = send(
    SMove(sourceKey, destKey, member)
  )
  
  /**
   * Removes and returns a random member from a set.
   *
   * @note This operation is similar to SRANDMEMBER, that returns a random element from a set but
   * does not remove it.
   *
   * @param key set key
   * @return random member, or $none if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sPop[K: Writer, R: Reader](key: K): Future[Option[R]] = send(SPop(key))
  
  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @return random member, or $none if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRandMember[K: Writer, R: Reader](key: K): Future[Option[R]] = send(SRandMember(key))
  
  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @param count number of member to randomly retrieve
   * @return set of random members, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 2.6.0
   */
  def sRandMembers[K: Writer, R: Reader](key: K, count: Int = 1): Future[Set[R]] = send(
    SRandMembers[K, R, Set](key, count)
  )
  
  /**
   * Removes one or more members from a set.
   *
   * @note Redis versions older than 2.4 can only remove one member per call.
   *
   * @param key set key
   * @param members members to remove from set
   * @return the number of members that were removed from the set, not including non-existing
   * members
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRem[K: Writer, W: Writer](key: K, members: W*): Future[Long] = send(SRem(key, members: _*))
  
  /**
   * Incrementally iterates the elements of a set.
   *
   * @param cursor the offset
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @return a pair containing the next cursor as its first element and the set of elements
   * as its second element
   *
   * @since 2.8.0
   */
  def sScan[K: Writer, R: Reader](
    key: K,
    cursor: Long,
    matchOpt: Option[String] = None,
    countOpt: Option[Int] = None
  ): Future[(Long, Set[R])] = send(
    SScan[K, R, Set](
      key = key,
      cursor = cursor,
      matchOpt = matchOpt,
      countOpt = countOpt
    )
  )
  
  /**
   * Computes the union of multiple sets.
   *
   * @param keys keys of sets to be included in the union computation
   * @return the resulting set, or the empty set if the first key does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnion[K: Writer, R: Reader](keys: K*): Future[Set[R]] = send(
    SUnion[K, R, Set](keys: _*)
  )
  
  /**
   * Computes the union of multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param keys keys of sets to be included in the union computation, if only one is specified,
   * it is simply copied to destKey
   * @return the cardinality of the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnionStore[KD: Writer, K: Writer](destKey: KD, keys: K*): Future[Long] = send(
    SUnionStore(destKey, keys: _*)
  )
  
}