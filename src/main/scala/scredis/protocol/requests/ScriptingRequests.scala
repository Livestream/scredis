package scredis.protocol.requests

import scredis.protocol._
import scredis.serialization.{ Reader, Writer }

object ScriptingRequests {
  
  import scredis.serialization.Implicits.stringReader
  
  private object Eval extends Command("EVAL")
  private object EvalSHA extends Command("EVALSHA")
  private object ScriptExists extends Command("SCRIPT EXISTS")
  private object ScriptFlush extends ZeroArgCommand("SCRIPT FLUSH")
  private object ScriptKill extends ZeroArgCommand("SCRIPT KILL")
  private object ScriptLoad extends Command("SCRIPT LOAD")
  
  case class Eval[R, W1: Writer, W2: Writer](
    script: String, keys: Seq[W1], args: Seq[W2]
  )(implicit decoder: Decoder[R]) extends Request[R](
    Eval,
    script,
    keys.size,
    keys.map(implicitly[Writer[W1]].write): _*,
    args.map(implicitly[Writer[W2]].write): _*
  ) {
    override def decode = decoder
  }
  
  case class EvalSHA[R, W1: Writer, W2: Writer](
    sha1: String, keys: Seq[W1], args: Seq[W2]
  )(implicit decoder: Decoder[R]) extends Request[R](
    EvalSHA,
    sha1,
    keys.size,
    keys.map(implicitly[Writer[W1]].write): _*,
    args.map(implicitly[Writer[W2]].write): _*
  ) {
    override def decode = decoder
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