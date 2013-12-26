package scredis.nio

import akka.actor._

import scredis._
import scredis.util.Logger
import scredis.protocol.{ NioProtocol }
import scredis.protocol.commands._
import scredis.parsing.Implicits.stringParser

import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress

object Nio {
  
  private val logger = Logger(getClass)
  
  def main(args: Array[String]): Unit = {
    val redis = Redis()
    
    val system = ActorSystem()
    val ioActor = system.actorOf(
      Props(classOf[IOActor], new InetSocketAddress("localhost", 6379))
        .withDispatcher("scredis.io-dispatcher")
    )
    val decoderActor = system.actorOf(
      Props(classOf[DecoderActor], ioActor).withDispatcher("scredis.io-dispatcher")
    )
    val encoderActor = system.actorOf(
      Props(classOf[EncoderActor], decoderActor).withDispatcher("scredis.io-dispatcher")
    )
    ioActor ! decoderActor
    
    val ioActor2 = system.actorOf(
      Props(classOf[IOActor], new InetSocketAddress("localhost", 6379))
        .withDispatcher("scredis.io-dispatcher")
    )
    val decoderActor2 = system.actorOf(
      Props(classOf[DecoderActor], ioActor2).withDispatcher("scredis.io-dispatcher")
    )
    val encoderActor2 = system.actorOf(
      Props(classOf[EncoderActor], decoderActor2).withDispatcher("scredis.io-dispatcher")
    )
    ioActor2 ! decoderActor2
    
    val ioActor3 = system.actorOf(
      Props(classOf[IOActor], new InetSocketAddress("localhost", 6379))
        .withDispatcher("scredis.io-dispatcher")
    )
    val decoderActor3 = system.actorOf(
      Props(classOf[DecoderActor], ioActor3).withDispatcher("scredis.io-dispatcher")
    )
    val encoderActor3 = system.actorOf(
      Props(classOf[EncoderActor], decoderActor3).withDispatcher("scredis.io-dispatcher")
    )
    ioActor3 ! decoderActor3
    
    import akka.routing._
    
    val router = system.actorOf(
      Props.empty.withRouter(RoundRobinRouter(routees = List(encoderActor, encoderActor2, encoderActor3)))
    )
    
    // WARMUP
    
    Await.result(
      Future.traverse((1 to 250000).toList)(i => {
        redis.get("key")
      })(List.canBuildFrom, redis.ec),
      5 second
    )
    
    Await.result(
      Future.traverse((1 to 250000).toList)(i => {
        NioProtocol.send(Get("key"))(router)
      })(List.canBuildFrom, system.dispatcher),
      5 second
    )
    
    println("WARMUP complete")
    val range = (1 to 2000000).toList
    /*
    val start1 = System.currentTimeMillis
    Await.result(
      Future.traverse(range)(i => {
        redis.get("key")
      })(List.canBuildFrom, redis.ec),
      20 second
    )
    val elapsed1 = System.currentTimeMillis - start1
    println("IO", elapsed1)*/
    
    println("START")
    
    val start2 = System.currentTimeMillis
    val f = Future.traverse(range)(i => {
      NioProtocol.send(Get("key"))(router)
    })(List.canBuildFrom, system.dispatcher)
    val mid2 = System.currentTimeMillis - start2
    println("NIO MID", mid2)
    Await.ready(
      f,
      30 second
    )
    val elapsed2 = System.currentTimeMillis - start2
    println("NIO", elapsed2)
    
    
  }
  
}