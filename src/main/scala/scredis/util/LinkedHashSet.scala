package scredis.util

import scala.collection.SortedSetLike
import scala.collection.generic._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.{ Builder, LinkedHashSet => MLinkedHashSet }

/**
 * Represents an '''immutable''' linked hash set.
 */
class LinkedHashSet[A](elems: A*) extends SortedSet[A]
  with SortedSetLike[A, LinkedHashSet[A]]
  with Serializable {

  override def stringPrefix = "LinkedHashSet"

  private val set = MLinkedHashSet[A](elems: _*)

  override def companion: GenericCompanion[LinkedHashSet] = LinkedHashSet
  override def empty = LinkedHashSet.empty

  def +(elem: A): LinkedHashSet[A] = {
    if (set.contains(elem)) this
    else {
      set += elem
      new LinkedHashSet(set.toSeq: _*)
    }
  }

  def -(elem: A): LinkedHashSet[A] = {
    if (set.contains(elem)) {
      set.remove(elem)
      new LinkedHashSet(set.toSeq: _*)
    } else this
  }

  def contains(elem: A): Boolean = set.contains(elem)

  def rangeImpl(from: Option[A], until: Option[A]): LinkedHashSet[A] = {
    (from, until) match {
      case (None, None) => this
      case (Some(from), None) => {
        val builder = LinkedHashSet.newBuilder[A]
        var fromFound = false
        this.foreach { elem =>
          if (fromFound) {
            builder += elem
          } else if (elem == from) {
            fromFound = true
            builder += elem
          }
        }
        builder.result()
      }
      case (None, Some(until)) => {
        val builder = LinkedHashSet.newBuilder[A]
        var untilFound = false
        this.foreach { elem =>
          if (elem == until) {
            return builder.result()
          } else {
            builder += elem
          }
        }
        builder.result()
      }
      case (Some(from), Some(until)) => {
        val builder = LinkedHashSet.newBuilder[A]
        var fromFound = false
        this.foreach { elem =>
          if (elem == until) {
            return builder.result()
          }
          
          if (fromFound) {
            builder += elem
          } else if (elem == from) {
            fromFound = true
            builder += elem
          }
        }
        builder.result()
      }
    }
  }
  
  def iterator = set.iterator
  
  def keysIteratorFrom(start: A): Iterator[A] = from(start).iterator
  
  def ordering = new Ordering[A] {
    def compare(a1: A, a2: A): Int = 0
  }
  
  def reverse = {
    val reversed = this.toList.reverse
    val builder = LinkedHashSet.newBuilder[A]
    reversed.foreach {
      builder += _
    }
    builder.result()
  }

}

object LinkedHashSet extends ImmutableSetFactory[LinkedHashSet] {
  
  class LinkedHashSetBuilder[A](empty: MLinkedHashSet[A]) extends Builder[A, LinkedHashSet[A]] {
    protected var elems: MLinkedHashSet[A] = empty
    def +=(x: A): this.type = { elems += x; this }
    def clear() { elems = empty }
    def result: LinkedHashSet[A] = new LinkedHashSet[A](elems.toList: _*)
  }
  
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, LinkedHashSet[A]] = setCanBuildFrom
  override def newBuilder[A]: Builder[A, LinkedHashSet[A]] =
    new LinkedHashSetBuilder[A](MLinkedHashSet.empty)
  override def empty[A]: LinkedHashSet[A] = new LinkedHashSet[A]
  def emptyInstance = new LinkedHashSet[Any]
}

