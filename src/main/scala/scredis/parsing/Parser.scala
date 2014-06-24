package scredis.parsing

import scredis.exceptions.RedisParsingException

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

/**
 * Represents the base class of all parsers. You can define new parsers by extending this class and
 * implementing the `parseImpl` method.
 * 
 * @note All parsing exceptions will be wrapped into a $e.
 * 
 * @define e [[scredis.exceptions.RedisParsingException]]
 * 
 */
abstract class Parser[A] {
  
  /**
   * Internal parse method to be implemented.
   * 
   * @param bytes the array of bytes
   * @return the parsed type
   */
  protected def parseImpl(bytes: Array[Byte]): A
  
  /**
   * Parses an array of bytes to the expected type.
   * 
   * @param bytes the array of bytes
   * @return the parsed type
   * @throws $e if an error occurs
   */
  final def parse(bytes: Array[Byte]): A = try {
    parseImpl(bytes)
  } catch {
    case e: Throwable => throw RedisParsingException(e)
  }
  
}

object RawParser extends Parser[Array[Byte]] {
  protected def parseImpl(bytes: Array[Byte]): Array[Byte] = bytes
}

object StringParser extends Parser[String] {
  protected def parseImpl(bytes: Array[Byte]): String = new String(bytes, "UTF-8")
}

object BooleanParser extends Parser[Boolean] {
  protected def parseImpl(bytes: Array[Byte]): Boolean = StringParser.parse(bytes).toBoolean
}

object ShortParser extends Parser[Short] {
  protected def parseImpl(bytes: Array[Byte]): Short = StringParser.parse(bytes).toShort
}

object IntParser extends Parser[Int] {
  protected def parseImpl(bytes: Array[Byte]): Int = StringParser.parse(bytes).toInt
}

object LongParser extends Parser[Long] {
  protected def parseImpl(bytes: Array[Byte]): Long = StringParser.parse(bytes).toLong
}

object FloatParser extends Parser[Float] {
  protected def parseImpl(bytes: Array[Byte]): Float = StringParser.parse(bytes).toFloat
}

object DoubleParser extends Parser[Double] {
  protected def parseImpl(bytes: Array[Byte]): Double = StringParser.parse(bytes).toDouble
}

