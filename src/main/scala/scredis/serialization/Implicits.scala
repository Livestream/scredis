package scredis.serialization

import java.util.UUID

object Implicits {
  implicit val bytesReader: Reader[Array[Byte]] = BytesReader
  implicit val stringReader: Reader[String] = UTF8StringReader
  implicit val booleanReader: Reader[Boolean] = BooleanReader
  implicit val shortReader: Reader[Short] = ShortReader
  implicit val intReader: Reader[Int] = IntReader
  implicit val longReader: Reader[Long] = LongReader
  implicit val floatReader: Reader[Float] = FloatReader
  implicit val doubleReader: Reader[Double] = DoubleReader
  implicit val uuidReader: Reader[UUID] = UUIDReader

  implicit val bytesWriter: Writer[Array[Byte]] = BytesWriter
  implicit val stringWriter: Writer[String] = UTF8StringWriter
  implicit val booleanWriter: Writer[Boolean] = BooleanWriter
  implicit val shortWriter: Writer[Short] = ShortWriter
  implicit val intWriter: Writer[Int] = IntWriter
  implicit val longWriter: Writer[Long] = LongWriter
  implicit val floatWriter: Writer[Float] = FloatWriter
  implicit val doubleWriter: Writer[Double] = DoubleWriter
  implicit val uuidWriter: Writer[UUID] = UUIDWriter
}