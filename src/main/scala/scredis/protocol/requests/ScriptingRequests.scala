package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

object ScriptingRequests {
  
  import scredis.serialization.Implicits.stringReader
  
  object Eval extends Command("EVAL")
  object EvalSHA extends Command("EVALSHA")
  object ScriptExists extends Command("SCRIPT", "EXISTS")
  object ScriptFlush extends ZeroArgCommand("SCRIPT", "FLUSH") with WriteCommand
  object ScriptKill extends ZeroArgCommand("SCRIPT", "KILL")
  object ScriptLoad extends Command("SCRIPT", "LOAD") with WriteCommand
  
  case class Eval[R, W1: Writer, W2: Writer](script: String, keys: Seq[W1], args: Seq[W2])(
    implicit decoder: Decoder[R], keysWriter: Writer[W1], argsWriter: Writer[W2]
  ) extends Request[R](
    Eval, script +: keys.size +: keys.map(keysWriter.write) ++: args.map(argsWriter.write): _*
  ) with Key {
    override def decode = decoder
    override val key =
      if (keys.isEmpty) "" // cluster client needs a key to choose a node. Empty string is hopefully good as any
      else new String(keysWriter.write(keys.head), Protocol.Encoding)
  }
  
  case class EvalSHA[R, W1: Writer, W2: Writer](sha1: String, keys: Seq[W1], args: Seq[W2])(
    implicit decoder: Decoder[R], keysWriter: Writer[W1], argsWriter: Writer[W2]
  ) extends Request[R](
    EvalSHA, sha1 +: keys.size +: keys.map(keysWriter.write) ++: args.map(argsWriter.write): _*
  ) with Key {
    override def decode = decoder
    override val key =
      if (keys.isEmpty) "" // cluster client needs a key to choose a node. Empty string is hopefully good as any
      else new String(keysWriter.write(keys.head), Protocol.Encoding)
  }
  
  case class ScriptExists(sha1s: String*) extends Request[Map[String, Boolean]](
    ScriptExists, sha1s: _*
  ) {
    override def decode = {
      case a: ArrayResponse => {
        val replies = a.parsed[Boolean, List] {
          case i: IntegerResponse => i.toBoolean
        }
        sha1s.zip(replies).toMap
      }
    }
  }
  
  case class ScriptFlush() extends Request[Unit](ScriptFlush) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class ScriptKill() extends Request[Unit](ScriptKill) {
    override def decode = {
      case SimpleStringResponse(_) => ()
    }
  }
  
  case class ScriptLoad(script: String) extends Request[String](ScriptLoad, script) {
    override def decode = {
      case b: BulkStringResponse => b.flattened[String]
    }
  }

}