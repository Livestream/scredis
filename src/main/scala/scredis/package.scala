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

import akka.dispatch.ExecutionContext

import org.apache.commons.lang3.concurrent.BasicThreadFactory

import java.util.concurrent.{ Executors, ExecutorService }

package object scredis {
  private var poolNumber = 0
  private var runningClients = 0

  private var executor: ExecutorService = _

  private def newThreadPool() = Executors.newFixedThreadPool(
    10,
    (new BasicThreadFactory.Builder()
      .namingPattern("scredis-global-queueing-%d")
      .build()
    )
  )

  private[scredis] def startClient(): Unit = synchronized {
    if(runningClients == 0) executor = newThreadPool()
    runningClients += 1
  }

  private[scredis] def stopClient(): Unit = synchronized {
    runningClients -= 1
    if(runningClients == 0) executor.shutdown()
  }

  private[scredis] def ec = ExecutionContext.fromExecutorService(executor)
  
  private[scredis] def newPoolNumber: Int = synchronized {
    poolNumber += 1
    poolNumber
  }
  
  val DefaultCommandOptions = CommandOptions()
}