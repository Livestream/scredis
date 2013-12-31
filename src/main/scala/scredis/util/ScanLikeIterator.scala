package scredis.util

class ScanLikeIterator[A, B[_] <: Iterable[_]](
  scan2: Long => (Long, B[A])
) extends Iterator[A] {
  private var cursor: Long = 0
  private var iterator: Iterator[A] = Iterator.empty
  
  private def scan(): Unit = {
    val (next, results) = scan2(cursor)
    cursor = next
    iterator = results.iterator.asInstanceOf[Iterator[A]]
  }
  
  def hasNext: Boolean = (iterator.hasNext || cursor > 0)
  def next(): A = {
    if (iterator.hasNext) {
      iterator.next()
    } else {
      scan()
      iterator.next()
    }
  }
  
  scan()
  
}

object ScanLikeIterator {
  
  def apply[A, B[_] <: Iterable[_]](scan: Long => (Long, B[A])): ScanLikeIterator[A, B] = {
    new ScanLikeIterator[A, B](scan)
  }
  
}