package scredis

import scredis.io.{ Connection, NonBlockingConnection }
import scredis.commands._
import scredis.protocol.Request
import scredis.exceptions.RedisTransactionBuilderException

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

final class TransactionBuilder private[scredis] ()(
  implicit val dispatcher: ExecutionContext
) extends Connection with NonBlockingConnection
  with ConnectionCommands
  with HashCommands
  with HyperLogLogCommands
  with KeyCommands
  with ListCommands
  with PubSubCommands
  with ScriptingCommands
  with ServerCommands
  with SetCommands
  with SortedSetCommands
  with StringCommands {
  
  private val requests = ListBuffer[Request[_]]()
  @volatile private var isClosed = false
  
  override protected def send[A](request: Request[A]): Future[A] = {
    if (isClosed) {
      throw RedisTransactionBuilderException(
        s"Cannot re-use a closed transaction builder; cannot queue '$request'"
      )
    }
    requests += request
    request.future
  }
  
  private[scredis] def result(): Transaction = {
    isClosed = true
    Transaction(requests.toList)
  }
  
}