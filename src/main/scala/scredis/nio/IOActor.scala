package scredis.nio

import akka.io.{ IO, Tcp }
import akka.actor.{ Actor, ActorRef }
import akka.util.ByteString

import scredis.util.Logger
import scredis.protocol.Request

import java.net.InetSocketAddress

class IOActor(remote: InetSocketAddress) extends Actor {
 
  import Tcp._
  import context.system
  
  private val logger = Logger(getClass)
  
  private var connection: ActorRef = _
  private var decoderActor: ActorRef = _
  
  private def stop(): Unit = {
    logger.trace("Stopping Actor...")
    context.stop(self)
  }
  
  def receive: Receive = {
    case decoderActor: ActorRef => {
      this.decoderActor = decoderActor
      logger.trace(s"Connecting to $remote...")
      IO(Tcp) ! Connect(remote)
      context.become(connecting)
    }
  }
  
  def connecting: Receive = {
    case c @ Connected(remote, local) => {
      logger.trace(s"Connected to $remote")
      connection = sender
      connection ! Register(self)
      context.become(ready)
    }
    case CommandFailed(_: Connect) => {
      logger.error(s"Could not connect to $remote")
      stop()
    }
  }
  
  def ready: Receive = {
    case Received(data) => {
      logger.trace(s"Received data: ${data.decodeString("UTF-8")}")
      decoderActor ! data
    }
    case request: Request[_, _] => {
      val data = ByteString(request.encoded)
      logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
      connection ! Write(data)
    }
    case requests: Seq[Request[_, _]] => {
      val data = requests.map(x => ByteString(x.encoded)).reduceLeft(_ ++ _)
      logger.trace(s"Writing data: ${data.decodeString("UTF-8")}")
      connection ! Write(data)
    }
    case CommandFailed(w: Write) => // O/S buffer was full
    case Close => {
      logger.trace(s"Closing connection...")
      connection ! Close
      context.become(closing)
    }
    case _: ConnectionClosed => {
      logger.debug(s"Connection has been closed by the server")
      stop()
    }
  }
  
  def closing: Receive = {
    case CommandFailed(c: CloseCommand) => {
      logger.warn(s"Connection could not be closed. Aborting...")
      connection ! Tcp.Abort
      stop()
    }
    case _: ConnectionClosed => {
      logger.debug(s"Connection has been closed")
      stop()
    }
  }
  
}
