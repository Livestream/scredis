package scredis.protocol.requests

import scredis.protocol._
import scredis.exceptions.RedisTransactionAbortedException

import scala.util.Try

object TransactionRequests {
  
  object Discard extends ZeroArgCommand("DISCARD") with WriteCommand
  object Exec extends ZeroArgCommand("EXEC") with WriteCommand
  object Multi extends ZeroArgCommand("MULTI") with WriteCommand
  object Unwatch extends ZeroArgCommand("UNWATCH") with WriteCommand
  object Watch extends Command("WATCH") with WriteCommand
  
  case class Discard() extends Request[Unit](Discard) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Exec(
    decoders: Seq[Decoder[Any]]
  ) extends Request[Vector[Try[Any]]](Exec) {
    override def decode = {
      case a: ArrayResponse         => a.parsed[Vector](decoders)
      case BulkStringResponse(None) => throw RedisTransactionAbortedException
    }
  }
  
  case class Multi() extends Request[Unit](Multi) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Unwatch() extends Request[Unit](Unwatch) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Watch(keys: String*) extends Request[Unit](Watch, keys: _*) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }

}