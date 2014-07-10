package scredis.serialization

import scredis.exceptions.RedisWriterException

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

/**
 * Represents the base class of all writers. You can define new writers by extending this class and
 * implementing the `writeImpl` method.
 * 
 * @note All reader exceptions will be wrapped into a $e.
 * 
 * @define e [[scredis.exceptions.RedisWriterException]]
 * 
 */
abstract class Writer[-A] {
  
  /**
   * Internal write method to be implemented.
   * 
   * @param value the object to be serialized
   * @return byte array of the serialized object
   */
  protected def writeImpl(value: A): Array[Byte]
  
  /**
   * Serializes an object into a byte array.
   * 
   * @param value the object to be serialized
   * @return byte array of the serialized object
   * @throws $e if an error occurs
   */
  final def write(value: A): Array[Byte] = try {
    writeImpl(value)
  } catch {
    case e: Throwable => throw RedisWriterException(e)
  }
  
}

object BytesWriter extends Writer[Array[Byte]] {
  protected def writeImpl(value: Array[Byte]): Array[Byte] = value
}

class StringWriter(charsetName: String) extends Writer[String] {
  protected def writeImpl(value: String): Array[Byte] = value.getBytes(charsetName)
}

object UTF8StringWriter extends StringWriter("UTF-8")

object BooleanWriter extends Writer[Boolean] {
  protected def writeImpl(value: Boolean): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object ShortWriter extends Writer[Short] {
  protected def writeImpl(value: Short): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object IntWriter extends Writer[Int] {
  protected def writeImpl(value: Int): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object LongWriter extends Writer[Long] {
  protected def writeImpl(value: Long): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object FloatWriter extends Writer[Float] {
  protected def writeImpl(value: Float): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object DoubleWriter extends Writer[Double] {
  protected def writeImpl(value: Double): Array[Byte] = UTF8StringWriter.write(value.toString())
}

object AnyWriter extends Writer[Any] {
  protected def writeImpl(value: Any): Array[Byte] = value match {
    case bytes: Array[Byte] => bytes
    case x                  => UTF8StringWriter.write(x.toString())
  }
}

