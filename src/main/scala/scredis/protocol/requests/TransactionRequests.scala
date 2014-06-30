package scredis.protocol.requests

import scredis.protocol._

import scala.util.Try

object TransactionRequests {
  
  private object Discard extends ZeroArgCommand("DISCARD")
  private object Exec extends ZeroArgCommand("EXEC")
  private object Multi extends ZeroArgCommand("MULTI")
  private object Unwatch extends ZeroArgCommand("UNWATCH")
  private object Watch extends Command("WATCH")
  
  case class Discard() extends Request[Unit](Discard) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Exec(
    parsers: Traversable[PartialFunction[Response, Any]]
  ) extends Request[IndexedSeq[Try[Any]]](Multi) {
    override def decode = {
      case a: ArrayResponse => a.parsed[IndexedSeq](parsers)
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