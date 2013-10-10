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
package scredis.protocol

import org.slf4j.LoggerFactory

import scredis.CommandOptions
import scredis.io.{ Connection, ConnectionException, ConnectionTimeoutException }
import scredis.exceptions._
import scredis.util.LinkedHashSet
import scredis.util.Pattern.retry
import scredis.parsing._
import scredis.parsing.Implicits._

import scala.collection.TraversableLike
import scala.collection.GenTraversableOnce
import scala.collection.generic.{ CanBuildFrom, GenericTraversableTemplate }
import scala.collection.mutable.{ ArrayBuilder, ListBuffer }

/**
 * This trait implements the `Redis` protocol.
 */
trait Protocol {
  private val Encoding = "UTF-8"
  private val StatusReply = '+'
  private val ErrorReply = '-'
  private val IntegerReply = ':'
  private val BulkReply = '$'
  private val MultiBulkReply = '*'

  private val StatusReplyByte = StatusReply.toByte
  private val ErrorReplyByte = ErrorReply.toByte
  private val IntegerReplyByte = IntegerReply.toByte
  private val BulkReplyByte = BulkReply.toByte
  private val MultiBulkReplyByte = MultiBulkReply.toByte

  private val CrLfByte = "\r\n".getBytes(Encoding)

  private val logger = LoggerFactory.getLogger(getClass)
  private val builder = new ArrayBuilder.ofByte()
  private val commands = ListBuffer[(Seq[Array[Byte]], (Char, Array[Byte]) => Any)]()
  private var isQueuing = false
  
  protected val DefaultCommandOptions: CommandOptions
  protected val connection: Connection

  protected def flattenKeyValueMap[K <: Any, V <: Any](
    before: List[String], map: Map[K, V]
  ): List[Any] = {
    val keyValues = collection.mutable.MutableList[Any]()
    keyValues ++= before
    for ((key, value) <- map) keyValues += key += value
    keyValues.toList
  }

  protected def flattenValueKeyMap[K <: Any, V <: Any](
    before: List[String], map: Map[K, V]
  ): List[Any] = {
    val keyValues = collection.mutable.MutableList[Any]()
    keyValues ++= before
    for ((key, value) <- map) keyValues += value += key
    keyValues.toList
  }
  
  private def serialize(arg: Any): Array[Byte] = arg match {
    case bytes: Array[Byte] => bytes
    case _ => arg.toString.getBytes(Encoding)
  }
  private def serialize(args: Seq[Any]): Seq[Array[Byte]] = args.map(serialize)

  private def multiBulkRequestFor(args: Seq[Array[Byte]]): Array[Byte] = {
    builder.clear()
    builder += MultiBulkReplyByte ++= args.size.toString.getBytes(Encoding) ++= CrLfByte
    for (arg <- args) {
      builder += BulkReplyByte ++= arg.size.toString.getBytes(Encoding) ++= CrLfByte
      builder ++= arg ++= CrLfByte
    }
    builder.result
  }

  private def multiBulkRequestFor(commands: List[Seq[Array[Byte]]]): Array[Byte] = {
    builder.clear()
    for (args <- commands) {
      builder += MultiBulkReplyByte ++= serialize(args.size) ++= CrLfByte
      for (arg <- args) {
        builder += BulkReplyByte ++= serialize(arg.size) ++= CrLfByte
        builder ++= arg ++= CrLfByte
      }
    }
    builder.result
  }

  private def parse(bytes: Array[Byte]): (Char, Array[Byte]) = {
    (bytes(0).toChar, bytes.drop(1))
  }

  private def checkReplyType(received: Char, expected: Char): Unit = {
    if (received != expected) throw RedisProtocolException(
      "Invalid reply type received -> expected: %c, received: %c", expected, received
    )
  }
  
  protected def send(data: Array[Byte]): Unit = {
    if(logger.isDebugEnabled) {
      logger.debug("Sending command: %s".format(StringParser.parse(data)))
    }
    connection.write(data)
  }
  protected def sendOnly(args: Any*): Unit = send(multiBulkRequestFor(serialize(args)))

  private[scredis] def startQueuing(): Unit = isQueuing = true

  private[scredis] def stopQueuing(): List[(Seq[Array[Byte]], (Char, Array[Byte]) => Any)] = {
    isQueuing = false
    val commands = this.commands.toList
    this.commands.clear()
    commands
  }
  
  private[scredis] def reconnectOnce[A](f: => A): A = try {
    f
  } catch {
    case e @ ConnectionException(_, _, true) => try {
      connection.connect()
      f
    } catch {
      case _: Throwable => throw RedisConnectionException(e, None)
    }
    case e @ ConnectionException(_, _, false) => throw RedisConnectionException(e, None)
    case e: ConnectionTimeoutException => throw RedisConnectionException(e, None)
  }

  private[scredis] def receive[A](as: (Char, Array[Byte]) => A): A = {
    val (replyType, bytes) = parse(connection.readLine())
    if (replyType == ErrorReply) throw RedisCommandException(StringParser.parse(bytes))
    as(replyType, bytes)
  }

  private[scredis] def send[A](args: Any*)(as: (Char, Array[Byte]) => A)(
    implicit opts: CommandOptions = DefaultCommandOptions, setTimeout: Boolean = true
  ): A = {
    if (isQueuing) {
      commands += ((serialize(args), as))
      null.asInstanceOf[A]
    } else {
      val currentTimeout = connection.currentTimeout
      if (setTimeout && currentTimeout != opts.timeout) connection.setTimeout(opts.timeout)
      
      if (opts.tries <= 1) reconnectOnce {
        sendOnly(args: _*)
        receive(as)
      } else {
        retry(opts.tries, opts.sleep) { count =>
          if (count == 1) reconnectOnce {
            sendOnly(args: _*)
            receive(as)
          } else try {
            sendOnly(args: _*)
            receive(as)
          } catch {
            case e: ConnectionException => throw RedisConnectionException(e, None)
            case e: ConnectionTimeoutException => throw RedisConnectionException(e, None)
          }
        }
      }
    }
  }
  
  private[scredis] def sendPipeline(commands: List[Seq[Array[Byte]]]): Unit =
    send(multiBulkRequestFor(commands))

  private[scredis] def receiveWithError[A](
    as: (Char, Array[Byte]) => A
  ): Either[RedisCommandException, Any] = try {
    Right(receive(as))
  } catch {
    case e: RedisCommandException => Left(e)
    case e: Throwable => throw e
  }

  private[scredis] def asUnit(replyType: Char, bytes: Array[Byte]): Unit = Unit

  private[scredis] def asAny(
    replyType: Char, bytes: Array[Byte]
  ): Option[String] = replyType match {
    case StatusReply => Some(asStatus(replyType, bytes))
    case IntegerReply => Some(asInteger(replyType, bytes).toString)
    case BulkReply => asBulk[String](replyType, bytes)
  }

  private[scredis] def asStatus(replyType: Char, bytes: Array[Byte]): String = {
    checkReplyType(replyType, StatusReply)
    StringParser.parse(bytes)
  }
  private[scredis] def asStatus[A](to: String => A)(replyType: Char, bytes: Array[Byte]): A =
    to(asStatus(replyType, bytes))

  private[scredis] def asOkStatus(replyType: Char, bytes: Array[Byte]): Unit = {
    val status = asStatus(replyType, bytes)
    if (status != "OK") throw RedisCommandException(status)
  }

  private[scredis] def asInteger(replyType: Char, bytes: Array[Byte]): Long = {
    checkReplyType(replyType, IntegerReply)
    longParser.parse(bytes)
  }
  private[scredis] def asInteger[A](to: Long => A)(replyType: Char, bytes: Array[Byte]): A =
    to(asInteger(replyType, bytes))

  private def asBulk[A](fn: Int => A)(replyType: Char, bytes: Array[Byte]): Option[A] = {
    checkReplyType(replyType, BulkReply)
    intParser.parse(bytes) match {
      case -1 => None
      case n => try {
        Some(fn(n))
      } catch {
        case e: RedisParsingException => throw e
        case e: Throwable => throw RedisConnectionException(e, None)
      }
    }
  }
  
  private[scredis] def asBulk[A](replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A]
  ): Option[A] = asBulk((n: Int) => parser.parse(connection.read(n)))(replyType, bytes)

  private[scredis] def asBulk[A, B](to: Option[A] => B)(replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A]
  ): B = to(asBulk[A](replyType, bytes))

  private[scredis] def asMultiBulk[A, B, C[X] <: Traversable[X]](as: (Char, Array[Byte]) => B)(
    replyType: Char, bytes: Array[Byte]
  )(
    implicit cbf: CanBuildFrom[Nothing, B, C[B]]
  ): C[B] = {
    checkReplyType(replyType, MultiBulkReply)
    val builder = cbf()
    intParser.parse(bytes) match {
      case -1 | 0 => // empty collection
      case n => for(i <- 1 to n) builder += receive(as)
    }
    builder.result
  }
  
  private[scredis] def asMultiBulk[A, B, C[X] <: Traversable[X], D](as: (Char, Array[Byte]) => B)(
    to: C[B] => D
  )(replyType: Char, bytes: Array[Byte])(
    implicit cbf: CanBuildFrom[Nothing, B, C[B]]
  ): D = to(asMultiBulk[A, B, C](as)(replyType, bytes))
  
  private[scredis] def asMultiBulk[A, B[X] <: Traversable[X]](replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A],
    cbf: CanBuildFrom[Nothing, Option[A], B[Option[A]]]
  ): B[Option[A]] = asMultiBulk[A, Option[A], B](asBulk[A])(replyType, bytes)
  
  private[scredis] def asMultiBulk[A, B[X] <: Traversable[X], C](to: B[Option[A]] => C)(
    replyType: Char, bytes: Array[Byte]
  )(
    implicit parser: Parser[A],
    cbf: CanBuildFrom[Nothing, Option[A], B[Option[A]]]
  ): C = to(asMultiBulk[A, Option[A], B](asBulk[A])(replyType, bytes))
  
  private[scredis] def asMultiBulk[A[X] <: Traversable[X], B](
    to: A[Option[Array[Byte]]] => B
  )(replyType: Char, bytes: Array[Byte])(
    implicit cbf: CanBuildFrom[Nothing, Option[Array[Byte]], A[Option[Array[Byte]]]]
  ): B = to(asMultiBulk[Array[Byte], A](replyType, bytes))
  
  private[scredis] def asMultiBulk[A](
    to: List[Option[Array[Byte]]] => A
  )(replyType: Char, bytes: Array[Byte]): A =
    asMultiBulk[List, A](to)(replyType, bytes)(List.canBuildFrom)

  private[scredis] def asBulkOrNullMultiBulkReply[A](replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A]
  ): Option[A] = try {
    asBulk[A](replyType, bytes)
  } catch {
    case e: RedisProtocolException => {
      assert(asMultiBulk[A, List](replyType, bytes).isEmpty)
      None
    }
    case e: Throwable => throw e
  }

  private[scredis] def asIntegerOrNullBulkReply(
    replyType: Char, bytes: Array[Byte]
  ): Option[Long] = try {
    Some(asInteger(replyType, bytes))
  } catch {
    case e: RedisProtocolException => {
      assert(asBulk[String](replyType, bytes).isEmpty)
      None
    }
    case e: Throwable => throw e
  }
  
  private[scredis] def asOkStatusOrNullBulkReply(
    replyType: Char, bytes: Array[Byte]
  ): Boolean = try {
    asOkStatus(replyType, bytes)
    true
  } catch {
    case e: RedisProtocolException => {
      assert(asBulk[String](replyType, bytes).isEmpty)
      false
    }
    case e: Throwable => throw e
  }

  private[scredis] def asBulkOrNestedMultiBulk[A, B[X] <: Traversable[X]](
    parser: Parser[A], cbf: CanBuildFrom[Nothing, Any, B[Any]]
  )(
    replyType: Char, bytes: Array[Byte]
  ): Any = try {
    asBulk[A](replyType, bytes)(parser).get
  } catch {
    case e: RedisParsingException => throw e
    case e: Throwable => asNestedMultiBulk(replyType, bytes)(parser, cbf)
  }

  private[scredis] def asNestedMultiBulk[A, B[X] <: Traversable[X]](
    replyType: Char, bytes: Array[Byte]
  )(
    implicit parser: Parser[A],
    cbf: CanBuildFrom[Nothing, Any, B[Any]]
  ): B[Any] = {
    checkReplyType(replyType, MultiBulkReply)
    val builder = cbf()
    intParser.parse(bytes) match {
      case -1 | 0 => // empty collection
      case n => for(i <- 1 to n) builder += receive(asBulkOrNestedMultiBulk(parser, cbf))
    }
    builder.result
  }

  private[scredis] def toBoolean(integer: Long): Boolean = (integer > 0)

  private[scredis] def toOptionalInt(integer: Long): Option[Int] = integer match {
    case -1 => None
    case _ => Some(integer.toInt)
  }

  private[scredis] def toOptionalLong(integer: Long): Option[Long] = integer match {
    case -1 => None
    case _ => Some(integer)
  }

  private[scredis] def toDouble(bulk: Option[String]): Double = bulk.get.toDouble

  private[scredis] def toOptionalDouble(bulk: Option[String]): Option[Double] =
    if (bulk.isDefined) Some(toDouble(bulk)) else None

  private[scredis] def flatten[A](option: Option[A]): A = option.get
    
  private[scredis] def flattenAll[A, B[X] <: Traversable[X]](
    option: Option[B[Option[A]]]
  )(
    implicit cbf: CanBuildFrom[Nothing, A, B[A]]
  ): B[A] = flattenElements[A, B](option)(cbf).get
  
  private[scredis] def flattenElements[A, B[X] <: Traversable[X]](
    option: Option[B[Option[A]]]
  )(
    implicit cbf: CanBuildFrom[Nothing, A, B[A]]
  ): Option[B[A]] = option.map { c =>
    val builder = cbf()
    for(x <- c) builder += x.get
    builder.result
  }


  private[scredis] def toLinkedHashSet(list: List[Option[String]]): LinkedHashSet[String] =
    LinkedHashSet(list.flatten: _*)

  private[scredis] def toPairsList[K, V](list: List[Option[Array[Byte]]])(
    implicit keyParser: Parser[K], valueParser: Parser[V]
  ): List[(K, V)] = list.grouped(2).collect {
    case List(Some(key), Some(value)) => (keyParser.parse(key), valueParser.parse(value))
  }.toList
  
  private[scredis] def toOptionalPairsList[K, V](list: List[Option[Array[Byte]]])(
    implicit keyParser: Parser[K], valueParser: Parser[V]
  ): Option[List[(K, V)]] = if(list.isEmpty) None else Some(toPairsList[K, V](list))

  private[scredis] def toPairsLinkedHashSet[K, V](list: List[Option[Array[Byte]]])(
    implicit keyParser: Parser[K], valueParser: Parser[V]
  ): LinkedHashSet[(K, V)] = LinkedHashSet(toPairsList[K, V](list): _*)

  private[scredis] def toMap[K, V](list: List[Option[Array[Byte]]])(
    implicit keyParser: Parser[K], valueParser: Parser[V]
  ): Map[K, V] = toPairsList[K, V](list).toMap
  
  private[scredis] def toOptionalMap[K, V](list: List[Option[Array[Byte]]])(
    implicit keyParser: Parser[K], valueParser: Parser[V]
  ): Option[Map[K, V]] = if(list.isEmpty) None else Some(toPairsList[K, V](list).toMap)

  private[scredis] def toMapWithKeys[K, V](keys: List[K])(list: List[Option[Array[Byte]]])(
    implicit parser: Parser[V]
  ): Map[K, V] = keys.zip(list).collect {
    case (key, Some(value)) => (key -> parser.parse(value))
  }.toMap
  
}

private[scredis] final class As private[scredis] (p: Protocol) {
  def asUnit(replyType: Char, bytes: Array[Byte]): Unit = p.asUnit(replyType, bytes)

  def asAny(replyType: Char, bytes: Array[Byte]): Option[String] = p.asAny(replyType, bytes)

  def asStatus(replyType: Char, bytes: Array[Byte]): String = p.asStatus(replyType, bytes)

  def asStatus[A](to: String => A)(replyType: Char, bytes: Array[Byte]): A =
    to(asStatus(replyType, bytes))

  def asOkStatus(replyType: Char, bytes: Array[Byte]): Unit = p.asOkStatus(replyType, bytes)

  def asInteger(replyType: Char, bytes: Array[Byte]): Long = p.asInteger(replyType, bytes)
  def asInteger[A](to: Long => A)(replyType: Char, bytes: Array[Byte]): A =
    to(asInteger(replyType, bytes))

  def asBulk[A](replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A]
  ): Option[A] = p.asBulk[A](replyType, bytes)

  def asBulk[A, B](to: Option[A] => B)(replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A]
  ): B = to(asBulk[A](replyType, bytes))

  def asMultiBulk[A, B, C[X] <: Traversable[X]](as: (Char, Array[Byte]) => B)(
    replyType: Char, bytes: Array[Byte]
  )(
    implicit cbf: CanBuildFrom[Nothing, B, C[B]]
  ): C[B] = p.asMultiBulk[A, B, C](as)(replyType, bytes)
  
  def asMultiBulk[A, B[X] <: Traversable[X]](replyType: Char, bytes: Array[Byte])(
    implicit parser: Parser[A],
    cbf: CanBuildFrom[Nothing, Option[A], B[Option[A]]]
  ): B[Option[A]] = asMultiBulk[A, Option[A], B](asBulk[A])(replyType, bytes)
  
  def asMultiBulk[A[X] <: Traversable[X], B](
    to: A[Option[Array[Byte]]] => B
  )(replyType: Char, bytes: Array[Byte])(
    implicit cbf: CanBuildFrom[Nothing, Option[Array[Byte]], A[Option[Array[Byte]]]]
  ): B = to(asMultiBulk[Array[Byte], A](replyType, bytes))
  
  def asMultiBulk[A](
    to: List[Option[Array[Byte]]] => A
  )(replyType: Char, bytes: Array[Byte]): A =
    asMultiBulk[List, A](to)(replyType, bytes)(List.canBuildFrom)
  
  /* Lua Conversions */

  def asBoolean(replyType: Char, bytes: Array[Byte]): Boolean =
    p.asIntegerOrNullBulkReply(replyType, bytes).isDefined

  def asString(replyType: Char, bytes: Array[Byte]): Option[String] =
    p.asBulk[String](replyType, bytes)

  def asList(replyType: Char, bytes: Array[Byte]): List[String] = {
    p.asMultiBulk[String, String, List](p.asBulk[String, String](p.flatten))(
      replyType, bytes
    )
  }

  def asNestedList(replyType: Char, bytes: Array[Byte]): List[Any] =
    p.asNestedMultiBulk(replyType, bytes)(StringParser, List.canBuildFrom)
}