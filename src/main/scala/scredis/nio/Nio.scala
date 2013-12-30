package scredis.nio

import akka.actor._
import akka.routing._

import scredis._
import scredis.util.Logger
import scredis.protocol.{ NioProtocol }
import scredis.protocol.commands._
import scredis.parsing.Implicits.stringParser

import scala.util.{ Try, Success, Failure }
import scala.concurrent.{ ExecutionContext, Future, Await }
import scala.concurrent.duration._

import java.net.InetSocketAddress

object Nio {
  
  private val logger = Logger(getClass)
  
  private val range = (1 to 3000000).toList
  
  private def run(target: ActorRef)(implicit ec: ExecutionContext): Long = {
    println("START")
    
    val start = System.currentTimeMillis
    val f = Future.traverse(range)(i => {
      NioProtocol.send(Get("key"))(target)
    })(List.canBuildFrom, ec)
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
    val redis = Redis()
    
    val system = ActorSystem()
    val ioActor = system.actorOf(
      Props(classOf[IOActor], new InetSocketAddress("localhost", 6379))
        .withDispatcher("scredis.io-dispatcher")
    )
    val partitionerActor = system.actorOf(
      Props(classOf[PartitionerActor], ioActor).withDispatcher("scredis.partitioner-dispatcher")
    )
    
    val encoderActor = system.actorOf(
      Props(classOf[EncoderActor], partitionerActor).withDispatcher("scredis.encoder-dispatcher")
    )
    
    ioActor ! partitionerActor
    
    implicit val dispatcher: ExecutionContext = system.dispatcher
    val target: ActorRef = encoderActor
    
    
    
    // WARMUP
    
    run(target)
    println("WARMUP complete")
    Thread.sleep(1000)
    
    val times = List(run(target), run(target), run(target))
    val avg = times.foldLeft(0L)(_ + _) / times.size
    val rps = range.size.toFloat / avg * 1000
    println(s"AVG: $rps r/s")
    system.shutdown()
  }
  
}