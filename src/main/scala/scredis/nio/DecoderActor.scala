package scredis.nio

import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.util.Logger
import scredis.protocol.{ NioProtocol, Request }

import scala.util.Try
import scala.collection.mutable.{ Queue => MQueue }

import java.nio.charset.StandardCharsets

class DecoderActor(pipelinerActor: ActorRef) extends Actor {
  
  private val charsetDecoder = StandardCharsets.UTF_8.newDecoder()
  private val requests = MQueue[Request[_, _]]()
  
  def receive: Receive = {
    case request: Request[_, _] => {
      requests.enqueue(request)
      pipelinerActor ! request
    }
    case data: ByteString => {
      val request = requests.dequeue()
      val reply = Try {
        NioProtocol.decode(data.asByteBuffer)
      }
      request.complete(reply)
    }
  }
  
}
