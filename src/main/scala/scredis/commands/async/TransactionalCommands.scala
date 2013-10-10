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
package scredis.commands.async

import akka.dispatch.{ ExecutionContext, Future }

import scredis.{ PipelineClient, TransactionalClient, CommandOptions }
import scredis.exceptions.RedisCommandException

import scala.collection.generic.CanBuildFrom

/**
 * This trait implements asynchronous transactional commands and pipelining.
 *
 * @author Alexandre Curreli
 * @define e [[scredis.exceptions.RedisCommandException]]
 * @define ep [[scredis.exceptions.RedisProtocolException]]
 * @define et [[scredis.exceptions.RedisTransactionException]]
 * @define p [[scredis.PipelineClient]]
 * @define m [[scredis.TransactionalClient]]
 */
trait TransactionalCommands extends Async {

  protected implicit val ec: ExecutionContext
  
  protected def force(opts: CommandOptions): CommandOptions = CommandOptions(
    opts.timeout,
    opts.tries,
    opts.sleep,
    true
  )

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
  def pipelined(
    body: PipelineClient => Any
  )(
    implicit opts: CommandOptions = DefaultCommandOptions
  ): Future[IndexedSeq[Either[RedisCommandException, Any]]] = async(_.pipelined(body))(force(opts))

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
  ): Future[A] = async(_.pipelined1(body))(force(opts))

  /**
   * Pipelines multiple commands and returns the results of several commands returned by `body`.
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
  def pipelinedN[A, B[_] <: Traversable[_]](body: PipelineClient => B[Future[A]])(
    implicit opts: CommandOptions = DefaultCommandOptions,
    cbf: CanBuildFrom[B[Future[A]], A, B[A]]
  ): Future[B[A]] = async(_.pipelinedN(body))(force(opts))

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
  ): Future[IndexedSeq[Either[RedisCommandException, Any]]] =
    async(_.transactional(body))(force(opts))

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
  ): Future[A] = async(_.transactional1(body))(force(opts))

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
    cbf: CanBuildFrom[B[Future[A]], A, B[A]]
  ): Future[B[A]] = async(_.transactionalN(body))(force(opts))

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
  ): Future[Unit] = async(_.watch(key, keys: _*))(force(opts))

  /**
   * Forgets about all watched keys.
   *
   * @param key key to watch
   * @param keys additional keys to watch
   *
   * @since 2.0.0
   */
  def unWatch()(implicit opts: CommandOptions = DefaultCommandOptions): Future[Unit] =
    async(_.unWatch())(force(opts))

}