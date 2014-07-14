package scredis

import scredis.protocol.Request
import scredis.protocol.requests.TransactionRequests.{ Multi, Exec }

import scala.util.{ Try, Success, Failure }
import scala.concurrent.ExecutionContext.Implicits.global

private[scredis] final case class Transaction (requests: Seq[Request[_]]) {
  val multiRequest = Multi()
  val execRequest = Exec(requests.map(_.decode))
  val future = execRequest.future
  
  future.onComplete {
    case Success(results) => {
      var i = 0
      requests.foreach { request =>
        if (!request.future.isCompleted) {
          results.apply(i) match {
            case Success(x) => request.success(x)
            case Failure(e) => request.failure(e)
          }
        }
        i += 1
      }
    }
    case Failure(e) => requests.foreach { request =>
      if (!request.future.isCompleted) {
        request.failure(e)
      }
    }
  }
  
}