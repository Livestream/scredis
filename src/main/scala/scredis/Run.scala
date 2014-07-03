package scredis

import akka.actor.ActorSystem

import scredis.serialization.Implicits._

import scala.util.{ Success, Failure }
import scala.concurrent.ExecutionContext.Implicits.global

object Run {
  def main(args: Array[String]): Unit = {
    println("RUN")
    implicit val system = ActorSystem("redis")
    val client = Client()
    
    client.set("key", "Yo!").onComplete {
      case Success(_) => println("SET")
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
}