package scredis

import org.scalameter.api._

import akka.actor.ActorSystem

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
/*
object ClientBenchmark extends PerformanceTest {
  
  private var system: ActorSystem = _
  private var client: Client = _
  
  /* configuration */
  lazy val executor = SeparateJvmsExecutor(
    new Executor.Warmer.Default,
    Aggregator.average,
    new Measurer.Default
  )
  lazy val reporter = Reporter.Composite(
    new RegressionReporter(
      RegressionReporter.Tester.Accepter(),
      RegressionReporter.Historian.Complete()
    ),
    HtmlReporter(true)
  )
  lazy val persistor = Persistor.None
  
  /* inputs */

  val sizes = Gen.range("size")(1000000, 3000000, 1000000)

  /* tests */

  performance of "Client" in {
    measure method "PING" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        system = ActorSystem()
        client = Client()(system)
      } tearDown { _ =>
        Await.result(client.quit(), 2 seconds)
        system.shutdown()
        client = null
        system = null
      } in { i =>
        implicit val ec = system.dispatcher
        val future = Future.traverse(1 to i) { i =>
          client.ping()
        }
        Await.result(future, 30 seconds)
      }
    }
    
    measure method "GET" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        system = ActorSystem()
        client = Client()(system)
        Await.result(client.set("foo", "bar"), 2 seconds)
      } tearDown { _ =>
        Await.result(client.del("foo"), 2 seconds)
        Await.result(client.quit(), 2 seconds)
        system.shutdown()
        client = null
        system = null
      } in { i =>
        implicit val ec = system.dispatcher
        val future = Future.traverse(1 to i) { i =>
          client.get("foo")
        }
        Await.result(future, 30 seconds)
      }
    }
    
    measure method "SET" in {
      using(sizes) config {
        exec.maxWarmupRuns -> 3
        exec.benchRuns -> 3
        exec.independentSamples -> 3
      } setUp { _ =>
        system = ActorSystem()
        client = Client()(system)
      } tearDown { _ =>
        Await.result(client.del("foo"), 2 seconds)
        Await.result(client.quit(), 2 seconds)
        system.shutdown()
        client = null
        system = null
      } in { i =>
        implicit val ec = system.dispatcher
        val future = Future.traverse(1 to i) { i =>
          client.set("foo", "bar")
        }
        Await.result(future, 30 seconds)
      }
    }
  }
  
}*/