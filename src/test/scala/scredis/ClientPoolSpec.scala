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
package scredis

import org.scalatest.{ WordSpec, GivenWhenThen, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers._

import akka.dispatch.{ Future, Await, ExecutionContext }
import akka.util.Duration

import scredis.exceptions.RedisCommandException
import scredis.tags._

import java.util.concurrent.Executors

class ClientPoolSpec extends WordSpec with GivenWhenThen with BeforeAndAfterAll {
  private val pool = ClientPool()()
  private val executor = Executors.newFixedThreadPool(10)
  private implicit val ec = ExecutionContext.fromExecutorService(executor)
  
  "A pool" when {
    "selecting a different database" should {
      "change the dabatase on all clients" taggedAs(V100) in {
        pool.withClient[Set[String]](_.keys("*")) must be('empty)
        pool.select(1)
        pool.withClient(_.set("STR", "Hello"))
        val f1 = for(i <- (1 to 1000)) yield {
          Future {
            pool.withClient[Option[String]](_.get("STR")) must be(Some("Hello"))
            pool.withClient[Set[String]](_.keys("*")) must be(Set("STR"))
          }
        }
        Await.result(Future.sequence(f1), Duration.Inf)
        pool.select(0)
        val f2 = for(i <- (1 to 1000)) yield {
          Future {
            pool.withClient[Option[String]](_.get("STR")) must be('empty)
            pool.withClient[Set[String]](_.keys("*")) must be('empty)
          }
        }
        Await.result(Future.sequence(f2), Duration.Inf)
      }
    }
  }
  
  override def afterAll() {
    pool.withClient(_.flushAll())
    executor.shutdown()
    pool.close()
  }
  
}