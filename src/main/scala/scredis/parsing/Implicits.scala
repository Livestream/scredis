package scredis.parsing

object Implicits {
  implicit val rawParser: Parser[Array[Byte]] = RawParser
  implicit val stringParser: Parser[String] = StringParser
  implicit val booleanParser: Parser[Boolean] = BooleanParser
  implicit val shortParser: Parser[Short] = ShortParser
  implicit val intParser: Parser[Int] = IntParser
  implicit val longParser: Parser[Long] = LongParser
  implicit val floatParser: Parser[Float] = FloatParser
  implicit val doubleParser: Parser[Double] = DoubleParser
}