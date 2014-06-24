package scredis.commands

import scredis.CommandOptions
import scredis.parsing._
import scredis.parsing.Implicits._
import scredis.protocol.Protocol

/**
 * This trait implements sets commands.
 *
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define none `None`
 */
trait SetCommands { self: Protocol =>
  import Names._

  /**
   * Adds one or more members to a set.
   *
   * @param key set key
   * @param member value to add
   * @param members additional values to add (only works for Redis >= 2.4)
   * @return the number of elements that were added to the set, not including all the elements
   * already present into the set
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sAdd(key: String, member: Any, members: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(SAdd, key,member) ++ members): _*)(asInteger)

  /**
   * Returns the number of members in a set.
   *
   * @param key set key
   * @return the cardinality (number of elements) of the set, or 0 if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sCard(key: String)(implicit opts: CommandOptions = DefaultCommandOptions): Long =
    send(SCard, key)(asInteger)

  /**
   * Returns the set resulting from the difference between the first set and all the successive
   * sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be substracted from first set
   * @param keys additional keys of sets to be substracted from first set
   * @return the resulting set, or the empty set if firstKey does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiff[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Set[A] = send((Seq(SDiff, firstKey, key) ++ keys): _*)(
    asMultiBulk[A, A, Set](asBulk[A, A](flatten))
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
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sDiffStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(SDiffStore, destKey, key) ++ keys): _*)(asInteger)

  /**
   * Intersects multiple sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be intersected with first set
   * @param keys additional keys of sets to be intersected with all sets
   * @return the resulting set, or the empty set if firstKey does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInter[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Set[A] = send((Seq(SInter, firstKey, key) ++ keys): _*)(
    asMultiBulk[A, A, Set](asBulk[A, A](flatten))
  )

  /**
   * Intersects multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param key key of first set
   * @param keys keys of sets to be intersected with all sets, if empty, first set is simply
   * copied to destKey
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sInterStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(SInterStore, destKey, key) ++ keys): _*)(asInteger)

  /**
   * Computes the union of multiple sets.
   *
   * @param firstKey key of first set
   * @param key key of set to be intersected with first set
   * @param keys additional keys of sets to be included in the union computation
   * @return the resulting set, or the empty set if firstKey does not exist
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnion[A](firstKey: String, key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Set[A] = send((Seq(SUnion, firstKey, key) ++ keys): _*)(
    asMultiBulk[A, A, Set](asBulk[A, A](flatten))
  )

  /**
   * Computes the union of multiple sets and stores the resulting set in a key.
   *
   * @note If destKey already exists, it is overwritten.
   *
   * @param destKey key where to store the resulting set
   * @param firstKey key of first set
   * @param keys keys of sets to be included in the union computation, if empty, first set is
   * simply copied to destKey
   * @return the number of elements in the resulting set
   * @throws $e if some keys contain a value that is not a set
   *
   * @since 1.0.0
   */
  def sUnionStore(destKey: String)(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(SUnionStore, destKey, key) ++ keys): _*)(asInteger)

  /**
   * Determines if a given value is a member of a set.
   *
   * @param key set key
   * @param member value to be tested
   * @return true if the provided value is a member of the set stored at key.
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sIsMember(key: String, member: Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(SIsMember, key,member)(asInteger(toBoolean))

  /**
   * Returns all the members of a set.
   *
   * @param key set key
   * @return set stored at key, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMembers[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Set[A] = send(SMembers, key)(asMultiBulk[A, A, Set](asBulk[A, A](flatten)))

  /**
   * Moves a member from one set to another.
   *
   * @param sourceKey key of source set
   * @param member value to be moved from source set to destination set
   * @param deskKey key of destination set
   * @return true if the member was moved, false if the element is not a member of source set and
   * no operation was performed
   * @throws $e if sourceKey or destKey contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sMove(sourceKey: String, member: Any)(destKey: String)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Boolean = send(SMove, sourceKey, destKey,member)(asInteger(toBoolean))

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
  def sPop[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Option[A] = send(SPop, key)(asBulk[A])

  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @return random member, or $none if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRandMember[A](key: String)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Option[A] = send(SRandMember, key)(asBulk[A])

  /**
   * Returns a random member from a set (without removing it).
   *
   * @param key set key
   * @param count number of member to randomly retrieve
   * @return set of random members, or the empty set if key does not exist
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRandMembers[A](key: String, count: Int = 1)(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): Set[A] = send(SRandMember, key, count)(
    asMultiBulk[A, A, Set](asBulk[A, A](flatten))
  )

  /**
   * Removes one or more members from a set.
   *
   * @note Redis versions older than 2.4 can only remove one member per call.
   *
   * @param key set key
   * @param member value to remove from set
   * @param members additional values to remove from set (only works with Redis >= 2.4)
   * @return the number of members that were removed from the set, not including non-existing
   * members
   * @throws $e if key contains a value that is not a set
   *
   * @since 1.0.0
   */
  def sRem(key: String, member: Any, members: Any*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Long = send((Seq(SRem, key,member) ++members): _*)(asInteger)
  
  /**
   * Incrementally iterates the elements of a set.
   *
   * @param cursor the offset
   * @param countOpt when defined, provides a hint of how many elements should be returned
   * @param matchOpt when defined, the command only returns elements matching the pattern
   * @return a pair containing the next cursor as its first element and the set of elements
   * as its second element
   *
   * @since 2.8.0
   */
  def sScan[A](key: String)(
    cursor: Long, countOpt: Option[Int] = None, matchOpt: Option[String] = None
  )(
    implicit opts: CommandOptions = DefaultCommandOptions,
    parser: Parser[A] = StringParser
  ): (Long, Set[A]) = send(
    generateScanLikeArgs(SScan, Some(key), cursor, countOpt, matchOpt): _*
  )(asScanMultiBulk[A, Set])
  
}