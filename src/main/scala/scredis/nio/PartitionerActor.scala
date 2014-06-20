package scredis.nio

import com.codahale.metrics.MetricRegistry

import akka.actor.{ Actor, ActorRef, Props }
import akka.routing._
import akka.util.ByteString

import scredis.protocol.{ Protocol, Request }

import scala.util.Success
import scala.collection.mutable.{ Queue => MQueue, ListBuffer }

import java.nio.ByteBuffer

class PartitionerActor(ioActor: ActorRef) extends Actor {
  
  private val requests = MQueue[Request[_]]()
  
  private val decoders = context.actorOf(
    SmallestMailboxPool(3).props(Props[DecoderActor]).withDispatcher("scredis.decoder-dispatcher")
  )
  
  private val tellTimer = scredis.protocol.NioProtocol.metrics.timer(
    MetricRegistry.name(getClass, "tellTimer")
  )
  
  private var remainingByteStringOpt: Option[ByteString] = None
  
  def receive: Receive = {
    case request: Request[_] => {
      requests.enqueue(request)
      ioActor ! request
    }
    case data: ByteString => {
      val completedData = remainingByteStringOpt match {
        case Some(remains) => {
          remainingByteStringOpt = None
          remains ++ data
        }
        case None => data
      }
      
      val buffer = completedData.asByteBuffer
      val repliesCount = Protocol.count(buffer)
      val position = buffer.position
      
      val trimmedData = if (buffer.remaining > 0) {
        remainingByteStringOpt = Some(ByteString(buffer))
        completedData.take(position)
      } else {
        completedData
      }
      
      val requests = ListBuffer[Request[_]]()
      for (i <- 1 to repliesCount) {
        requests += this.requests.dequeue()
      }
      
      decoders ! Partition(trimmedData, requests.toList.iterator)
    }
  }
  
}
