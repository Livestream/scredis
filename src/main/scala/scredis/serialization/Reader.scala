package scredis.serialization

import java.util.UUID

import scredis.exceptions.RedisReaderException

/**
 * Represents the base class of all readers. You can define new readers by extending this class and
 * implementing the `readImpl` method.
 * 
 * @note All reader exceptions will be wrapped into a $e.
 * 
 * @define e [[scredis.exceptions.RedisReaderException]]
 * 
 */
trait Reader[A] {
  
  /**
   * Internal read method to be implemented.
   * 
   * @param bytes the array of bytes
   * @return the deserialized type
   */
  protected def readImpl(bytes: Array[Byte]): A
  
  /**
   * Deserializes an array of bytes to the expected type.
   * 
   * @param bytes the array of bytes
   * @return the deserialized type
   * @throws $e if an error occurs
   */
  final def read(bytes: Array[Byte]): A = try {
    readImpl(bytes)
  } catch {
    case e: Throwable => throw RedisReaderException(e)
  }
  
}

object BytesReader extends Reader[Array[Byte]] {
  protected def readImpl(bytes: Array[Byte]): Array[Byte] = bytes
}

class StringReader(charsetName: String) extends Reader[String] {
  protected def readImpl(bytes: Array[Byte]): String = new String(bytes, charsetName)
}

object UTF8StringReader extends StringReader("UTF-8")

object BooleanReader extends Reader[Boolean] {
  protected def readImpl(bytes: Array[Byte]): Boolean = UTF8StringReader.read(bytes).toBoolean
}

object ShortReader extends Reader[Short] {
  protected def readImpl(bytes: Array[Byte]): Short = UTF8StringReader.read(bytes).toShort
}

object IntReader extends Reader[Int] {
  protected def readImpl(bytes: Array[Byte]): Int = UTF8StringReader.read(bytes).toInt
}

object LongReader extends Reader[Long] {
  protected def readImpl(bytes: Array[Byte]): Long = UTF8StringReader.read(bytes).toLong
}

object FloatReader extends Reader[Float] {
  protected def readImpl(bytes: Array[Byte]): Float = UTF8StringReader.read(bytes).toFloat
}

object DoubleReader extends Reader[Double] {
  protected def readImpl(bytes: Array[Byte]): Double = UTF8StringReader.read(bytes).toDouble
}

object UUIDReader extends Reader[UUID] {
  protected def readImpl(bytes: Array[Byte]): UUID =
    bytes.length == 16 match {
      case false => null
      case true =>
        var msb = 0L
        var lsb = 0L
        for (i <- 0 until 8) {
          msb = (msb << 8) | (bytes(i) & 0xff)
        }
        for (i <- 8 until 16) {
          lsb = (lsb << 8) | (bytes(i) & 0xff)
        }
        new UUID(msb, lsb)
    }
}

