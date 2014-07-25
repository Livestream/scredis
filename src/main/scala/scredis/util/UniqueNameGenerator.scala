package scredis.util

import scala.collection.mutable.{ Map => MMap }

import java.util.concurrent.atomic.AtomicInteger

object UniqueNameGenerator {
  private val ids = MMap[String, AtomicInteger]()
  
  def getUniqueName(name: String): String = {
    ids.synchronized {
      val counter = ids.getOrElseUpdate(name, new AtomicInteger(0))
      counter.incrementAndGet()
      if (counter.get == 1) {
        name
      } else {
        s"$name-${counter.get}"
      }
    }
  }
  
  def getNumberedName(name: String): String = {
    val counter = ids.synchronized {
      ids.getOrElseUpdate(name, new AtomicInteger(0))
    }
    s"$name-${counter.incrementAndGet()}"
  }
  
}