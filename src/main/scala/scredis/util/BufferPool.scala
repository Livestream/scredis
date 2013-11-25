package scredis.util

import scala.annotation.tailrec

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

class BufferPool(maxEntries: Int) {
  private[this] val locked = new AtomicBoolean(false)
  private[this] val pool = new Array[ByteBuffer](maxEntries)
  private[this] var size: Int = 0

  private def allocate(length: Int): ByteBuffer = ByteBuffer.allocate(length)
  
  @tailrec
  final def acquire(length: Int): ByteBuffer = {
    if (locked.compareAndSet(false, true)) {
      val buffer = try {
        if (size > 0) {
          size -= 1
          pool(size)
        } else {
          null
        }
      } finally {
        locked.set(false)
      }

      if (buffer == null) {
        allocate(length)
      } else {
        buffer.clear()
        buffer
      }
    } else {
      acquire(length)
    }
  }

  @tailrec
  final def release(buffer: ByteBuffer): Unit = {
    if (locked.compareAndSet(false, true)) {
      try {
        if (size < maxEntries) {
          pool(size) = buffer
          size += 1
        }
        // else let the buffer get garbage collected
      }
      finally {
        locked.set(false)
      }
    } else {
      release(buffer)
    }
  }
  
}