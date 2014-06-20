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
package scredis.commands

import scredis.{ CommandOptions, Client, PipelineClient, TransactionalClient }
import scredis.exceptions.RedisCommandException

import scala.concurrent.{ Await, Future, ExecutionContext }
import scala.concurrent.duration.Duration
import scala.collection.generic.CanBuildFrom
import scala.util.Try

/**
 * This trait implements transactional commands and pipelining.
 *
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define ep [[scredis.exceptions.RedisProtocolException]]
 * @define et [[scredis.exceptions.RedisTransactionException]]
 * @define p [[scredis.PipelineClient]]
 * @define m [[scredis.TransactionalClient]]
 */
trait TransactionCommands { self: Client =>
  import Names._

  protected val Never = Duration.Inf

  /**
   * Creates and returns a $p used to send multiple commands at once. Calling `sync()` on $p
   * triggers the execution of all queued commands.
   *
   * @return $p
   *
   * @since 1.0.0
   */
  def pipeline(): PipelineClient = new PipelineClient(this)

  /**
   * Pipelines multiple commands and returns the results.
   *
   * @note The `sync()` method must not be called within `body` as it will automatically be called
   * immediately after `body` returns.
   *
   * @param body function to be executed with the provided $p
   * @return an indexed sequence containing the results of the issued commands in the same order
   * they were queued
   * @throws $ep if `sync()` is called on the $p within `body`
   *
   * @since 1.0.0
   */
  def pipelined(body: PipelineClient => Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): IndexedSeq[Try[Any]] = {
    val p = pipeline()
    body(p)
    p.sync()
  }

  /**
   * Pipelines multiple commands and returns the result of one command returned by `body`.
   *
   * @note It is safe to call `sync()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $p
   * @return an indexed sequence containing the results of the issued commands in the same order
   * they were queued
   *
   * @since 1.0.0
   */
  def pipelined1[A](body: PipelineClient => Future[A])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = {
    val p = pipeline()
    val result = body(p)
    if (p.isClosed) Await.result(result, Never)
    else {
      p.sync()
      Await.result(result, Never)
    }
  }

  /**
   * Pipelines multiple commands and returns the results of several commands returned by `body`.
   *
   * @note It is safe to call `sync()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $p
   * @return collection containing the results of the issued commands in the same order
   * they were queued
   *
   * @since 1.0.0
   */
  def pipelinedN[A, B[_] <: Traversable[_]](body: PipelineClient => B[Future[A]])(
    implicit opts: CommandOptions = DefaultCommandOptions,
    cbf: CanBuildFrom[B[Future[A]], A, B[A]],
    ec: ExecutionContext
  ): B[A] = {
    val p = pipeline()
    val result = Future.sequence(body(p))
    if (p.isClosed) Await.result(result, Never)
    else {
      p.sync()
      Await.result(result, Never)
    }
  }
  
  /**
   * Pipelines multiple commands and returns the results as a map of key to command result pairs.
   *
   * @note It is safe to call `sync()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $p
   * @return map of key to command pairs
   *
   * @since 1.0.0
   */
  def pipelinedM[K, V](body: PipelineClient => Map[K, Future[V]])(
    implicit opts: CommandOptions = DefaultCommandOptions,
    ec: ExecutionContext
  ): Map[K, V] = {
    val p = pipeline()
    val result = body(p)
    if (!p.isClosed) {
      p.sync()
    }
    for ((key, future) <- result) yield {
      (key -> Await.result(future, Never))
    }
  }

  /**
   * Creates and returns a $m used to perform a transaction. Calling `exec()` on $m triggers the
   * execution of all queued commands as part of a Redis transaction.
   *
   * @return $m
   * @throws $e if a previous transaction is still running, i.e. if `exec()` has not been called on
   * a previous `multi()`
   *
   * @since 2.0.0
   */
  def multi()(implicit opts: CommandOptions = DefaultCommandOptions): TransactionalClient = {
    send(Multi)(asUnit)
    new TransactionalClient(this)
  }

  /**
   * Performs multiple commands as part of a Redis transaction and returns the results.
   *
   * @note The `exec()` method must not be called within `body` as it will automatically be called
   * immediately after `body` returns.
   *
   * @param body function to be executed with the provided $m
   * @return an indexed sequence containing the results of the issued commands in the same order
   * they were queued
   * @throws $ep if `exec()` is called on the $m within `body`
   * @throws $et if the transaction is discarded within `body` or if a watched key has been
   * modified by another client
   *
   * @since 2.0.0
   */
  def transactional(body: TransactionalClient => Any)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): IndexedSeq[Try[Any]] = {
    val m = multi()
    body(m)
    m.exec()
  }

  /**
   * Performs multiple commands as part of a Redis transaction and returns the result of the
   * command returned by `body`.
   *
   * @note It is safe to call `exec()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $m
   * @return an indexed sequence containing the results of the issued commands in the same order
   * they were queued
   * @throws $et if a watched key has been modified by another client
   *
   * @since 2.0.0
   */
  def transactional1[A](body: TransactionalClient => Future[A])(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): A = {
    val m = multi()
    val result = body(m)
    if (m.isClosed) Await.result(result, Never)
    else {
      m.exec()
      Await.result(result, Never)
    }
  }

  /**
   * Performs multiple commands as part of a Redis transaction and returns the results of several
   * commands returned by `body`.
   *
   * @note It is safe to call `exec()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $m
   * @return an indexed sequence containing the results of the issued commands in the same order
   * they were queued
   * @throws $et if a watched key has been modified by another client
   *
   * @since 2.0.0
   */
  def transactionalN[A, B[_] <: Traversable[_]](body: TransactionalClient => B[Future[A]])(
    implicit opts: CommandOptions = DefaultCommandOptions,
    cbf: CanBuildFrom[B[Future[A]], A, B[A]],
    ec: ExecutionContext
  ): B[A] = {
    val m = multi()
    val result = Future.sequence(body(m))
    if (m.isClosed) Await.result(result, Never)
    else {
      m.exec()
      Await.result(result, Never)
    }
  }
  
  /**
   * Performs multiple commands as part of a Redis transaction and returns the results as a map
   * of key to command result pairs.
   *
   * @note It is safe to call `exec()` within `body` as the library will detect whether it has
   * already been called and won't call it a second time.
   *
   * @param body function to be executed with the provided $m
   * @return map of key to command pairs
   * @throws $et if a watched key has been modified by another client
   *
   * @since 1.0.0
   */
  def transactionalM[K, V](body: TransactionalClient => Map[K, Future[V]])(
    implicit opts: CommandOptions = DefaultCommandOptions,
    ec: ExecutionContext
  ): Map[K, V] = {
    val m = multi()
    val result = body(m)
    if (!m.isClosed) {
      m.exec()
    }
    for ((key, future) <- result) yield {
      (key -> Await.result(future, Never))
    }
  }

  /**
   * Watches the given keys, which upon modification, will abort a transaction.
   *
   * @param key key to watch
   * @param keys additional keys to watch
   *
   * @since 2.0.0
   */
  def watch(key: String, keys: String*)(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Unit = send((Watch :: key :: keys.toList): _*)(asUnit)

  /**
   * Forgets about all watched keys.
   *
   * @param key key to watch
   * @param keys additional keys to watch
   *
   * @since 2.0.0
   */
  def unWatch()(implicit opts: CommandOptions = DefaultCommandOptions): Unit = send(UnWatch)(asUnit)

}