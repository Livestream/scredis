package scredis.nio

import akka.actor._

import scredis.util.Logger
import scredis.protocol.{ NioProtocol, SimpleRequest, TransformableRequest }
import scredis.protocol.commands._

import scala.util.{ Try, Success, Failure }
import scala.concurrent.duration._

import java.net.InetSocketAddress

object Nio {
  
  private val logger = Logger(getClass)
  
  def main(args: Array[String]): Unit = {
    val system = ActorSystem()
    val ioActor = system.actorOf(Props(classOf[IOActor], new InetSocketAddress("localhost", 6379)))
    val pipelinerActor = system.actorOf(
      Props(classOf[PipelinerActor], ioActor, 100 milliseconds, Some(5))
    )
    val decoderActor = system.actorOf(Props(classOf[DecoderActor], pipelinerActor))
    implicit val encoderActor = system.actorOf(Props(classOf[EncoderActor], decoderActor))
    ioActor ! decoderActor
    
    import system.dispatcher
    
    NioProtocol.send(
      TransformableRequest(Get, List("key"))(x => x.map(new String(_, "UTF-8")))
    ).onComplete {
      case Success(x) => println(x)
      case Failure(e) => throw e
    }
  }
  
}