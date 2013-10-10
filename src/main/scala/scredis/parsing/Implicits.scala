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