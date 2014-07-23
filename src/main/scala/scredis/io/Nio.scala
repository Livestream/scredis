package scredis.io

import akka.actor._
import akka.routing._

import scredis._
import scredis.protocol._
import scredis.protocol.requests.ConnectionRequests.Ping
import scredis.serialization.Implicits.stringReader

import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress

object Nio {
  
  private val range = (1 to 3000000).toList
  
  private def run(client: Client)(implicit ec: ExecutionContext): Long = {
    println("START")
    
    val start = System.currentTimeMillis
    val f = Future.traverse(range) { i =>
      client.ping()
    }
    println("QUEUING DONE")
    Await.ready(
      f,
      Duration.Inf
    )
    val elapsed = System.currentTimeMillis - start
    println(elapsed)
    elapsed
  }
  
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    val client = Client()
    
    implicit val dispatcher: ExecutionContext = system.dispatcher
    
    // WARMUP
    run(client)
    println("WARMUP complete")
    Thread.sleep(1000)
    
    val times = List(run(client), run(client), run(client))
    val avg = times.foldLeft(0L)(_ + _) / times.size
    val rps = range.size.toFloat / avg * 1000
    println(s"AVG: $rps r/s")
    system.shutdown()
  }
  
}