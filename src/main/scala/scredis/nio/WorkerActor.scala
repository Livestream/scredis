package scredis.nio

import akka.io.{ IO, Tcp }
import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.parsing.Implicits.stringParser
import scredis.io.Connection
import scredis.CommandOptions
import scredis.util.Logger
import scredis.protocol.Protocol

import java.net.InetSocketAddress

class WorkerActor() extends Actor with Protocol {
 
  import Tcp._
  import context.system
  
  private val logger = Logger(getClass)
  
  protected val DefaultCommandOptions: CommandOptions = CommandOptions()
  protected val connection: Connection = null
  
  private def stop(): Unit = {
    logger.trace("Stopping Actor...")
    context.stop(self)
  }
  
  def receive: Receive = {
    case data: ByteString => {
      val char = data.head.toChar
      val command = data.tail.asByteBuffer.array
      val x = asBulk[String](char, command)
      println(x)
    }
  }
  
}
