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
package scredis.util

import scala.collection.SetLike
import scala.collection.generic._
import scala.collection.mutable.{ Builder, LinkedHashSet => MLinkedHashSet }

/**
 * Represents an '''immutable''' linked hash set.
 */
class LinkedHashSet[A](elems: A*) extends Set[A]
  with GenericSetTemplate[A, LinkedHashSet]
  with SetLike[A, LinkedHashSet[A]]
  with Serializable {

  override def stringPrefix = "LinkedHashSet"

  private val set = MLinkedHashSet[A](elems: _*)

  override def companion: GenericCompanion[LinkedHashSet] = LinkedHashSet

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

  def iterator = set.iterator

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

