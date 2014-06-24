package scredis.protocol.requests

import scredis.protocol._

object ConnectionRequests {
  
  private object Auth extends Command("AUTH")
  private object Echo extends Command("ECHO")
  private object Ping extends ZeroArgCommand("PING")
  private object Quit extends ZeroArgCommand("QUIT")
  private object Select extends Command("SELECT")
  
  case class Auth(password: String) extends Request[Unit](Auth, password) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Echo(message: String) extends Request[String](Echo, message) {
    override def decode = {  
      case SimpleStringResponse(value) => value
    }
  }
  
  case class Ping() extends Request[String](Ping) {
    override def decode = {  
      case SimpleStringResponse(value) => value
    }
  }
  
  case class Quit() extends Request[Unit](Quit) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }
  
  case class Select(database: Int) extends Request[Unit](Select, database) {
    override def decode = {  
      case SimpleStringResponse(value) => ()
    }
  }

}