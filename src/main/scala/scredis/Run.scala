package scredis

import akka.actor.ActorSystem

import scredis.serialization.Implicits._

import scala.util.{ Success, Failure }
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Run {
  
  private def testReconnection(): Unit = {
    println("TEST RECONNECTION")
    implicit val system = ActorSystem("redis")
    val client = Client()
    
    println("SET")
    client.set("key", "Yo!").onComplete {
      case Success(_) => println("SET DONE")
      case Failure(e) => e.printStackTrace()
    }
    
    while (true) {
      println("GET")
      client.get[String]("key").onComplete {
        case Success(keyOpt) => println(keyOpt)
        case Failure(e) => e.printStackTrace()
      }
      Thread.sleep(1000)
    }
  }
  
  private def testPerformance(): Unit = {
    println("TEST PERFORMANCE")
    val count = 5000000
    implicit val system = ActorSystem("redis")
    val client = Client()
    
    val warmup = Future.traverse((1 to 100000)) { i =>
      client.ping()
    }
    Await.result(warmup, Duration.Inf)
    println("WARMUP COMPLETE")
    
    val start = System.currentTimeMillis
    val future = Future.traverse((1 to count)) { i =>
      client.ping()
    }
    Await.result(future, Duration.Inf)
    val elapsed = System.currentTimeMillis - start
    val rps = count.toFloat / (elapsed / 1000)
    println("DONE", elapsed, rps)
  }
  
  def main(args: Array[String]): Unit = {
    //testReconnection()
    implicit val system = ActorSystem()
    val client = Client()
    val client1 = Client(port = 6380, passwordOpt = Some("foobar"))
    println(Await.result(client1.ping(), 1 second))
    System.exit(0)
  }
  
}