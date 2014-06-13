package scredis.util

import scala.concurrent.ExecutionContext

import java.util.concurrent.{ Executor, Executors, RejectedExecutionException, Semaphore }

class BlockingExecutor(executor: Executor, maxConcurrent: Int) extends ExecutionContext {
  
  private val semaphore = new Semaphore(maxConcurrent)
  
  override def reportFailure(exception: Throwable): Unit = ExecutionContext.defaultReporter.apply(
    exception
  )
  
  override def execute(runnable: Runnable): Unit = {
    semaphore.acquire()
    try {
      executor.execute(
        new Runnable() {
          override def run(): Unit = {
            try {
              runnable.run()
            } finally {
              semaphore.release()
            }
          }
        }
      )
    } catch {
      case e: RejectedExecutionException => {
        semaphore.release()
        throw e
      }
    }
  }
  
}

object BlockingExecutor {
  def apply(executor: Executor, maxConcurrent: Int) = new BlockingExecutor(
    executor, maxConcurrent
  )
}