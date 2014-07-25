package scredis.util

import scala.annotation.tailrec

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

class BufferPool(maxCapacity: Int, maxBufferSize: Int, isDirect: Boolean = false) {
  private[this] val locked = new AtomicBoolean(false)
  private[this] val pool = new Array[ByteBuffer](maxCapacity)
  private[this] var size: Int = 0
  
  @inline
  private def allocate(length: Int): ByteBuffer = if (isDirect) {
    ByteBuffer.allocateDirect(length)
  } else {
    ByteBuffer.allocate(length)
  }
  
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

      if (buffer == null || buffer.capacity < length) {
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
    if (buffer.capacity > maxBufferSize) {
      return
    }
    
    if (locked.compareAndSet(false, true)) {
      try {
        if (size < maxCapacity) {
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