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
package scredis.exceptions

/**
 * Base class of all Redis exceptions.
 */
sealed abstract class RedisException(
  cause: Option[Throwable],
  message: Option[String],
  objects: Any*
) extends Exception {
  
  override def getMessage: String = message match {
    case Some(m) => objects.size match {
      case 0 => m
      case _ => m.format(objects: _*)
    }
    case None => cause match {
      case Some(e) => e.toString
      case None => null
    }
  }
  
  cause.foreach(initCause)
  
}

/**
 * Exception resulting from a connection error, e.g. connection refused, timeouts, etc.
 * All connection and socket related exceptions will be wrapped into it.
 */
final case class RedisConnectionException(
  e: Throwable,
  message: Option[String],
  objects: Any*
) extends RedisException(Some(e), message, objects: _*)

/**
 * Exception thrown when the Redis server replies with an error message
 */
final case class RedisCommandException(message: String) extends RedisException(None, Some(message))

/**
 * Exception thrown when the input could not be parsed by the provided [[scredis.parsing.Parser]].
 */
final case class RedisParsingException(e: Throwable) extends RedisException(Some(e), None)

/**
 * Exception resulting from a transaction being discarded.
 */
final case class RedisTransactionException(message: String) extends RedisException(
  None,
  Some(message)
)

/**
 * Exception resulting from an unexpected breach of protocol such as receiving an unexpected reply
 * from the `Redis` server.
 */
final case class RedisProtocolException(message: String, objects: Any*) extends RedisException(
  None,
  Some(message),
  objects: _*
)