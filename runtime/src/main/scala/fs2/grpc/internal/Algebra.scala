package fs2.grpc.internal

import cats.syntax.all._
import cats.effect.std.Dispatcher
import cats.effect.{Fiber, Sync, SyncIO}
import io.grpc.{Metadata, Status}

import scala.concurrent.Future

// ---- REMOTE INPUT ----
// inputs originating from the remote code (i.e. coming over the wire)
object RemoteInput {
  sealed trait Server[+Request]
  sealed trait Client[+Response]

  case object Ready extends Server[Nothing] with Client[Nothing]
  case class Message[A](value: A) extends Server[A] with Client[A]
  case object HalfClose extends Server[Nothing]
  case object Cancel extends Server[Nothing]
  case class Close(status: Status, trailers: Metadata) extends Client[Nothing]
  case object Complete extends Server[Nothing]
}


//  ---- LOCAL INPUT ----
// inputs originating from the local code (i.e. the application)


object LocalInput {
  sealed trait Server[+Response]
  sealed trait Client[+Request]
  sealed trait ClientCommon

//  case class Message[A](value: A) extends Server[A] with Client[A]
  case class RequestMore(n: Int) extends Server[Nothing] with Client[Nothing] with ClientCommon
  case object HalfClose extends Client[Nothing]

  // client can cancel or terminate, server can only terminate
  case object Cancel extends Client[Nothing] with ClientCommon
}


// ---- REMOTE OUTPUT ----
// outputs to be sent to the remote side


object RemoteOutput {
  sealed trait Server[+Response]
  sealed trait Client[+Request]

  case class Message[A](value: A) extends Server[A] with Client[A]
  case class RequestMore(n: Int) extends Server[Nothing] with Client[Nothing]
  case class Close(status: Status, trailers: Option[Metadata] = None) extends Server[Nothing]

  // TODO could re-use HalfClose as Close(OK) in server context if that's useful?
  case object Close extends Server[Nothing]
  case object HalfClose extends Client[Nothing]
  case object Cancel extends Client[Nothing]
}

//// ---- LOCAL OUTPUT ----
//// Outputs to be sent to ... self?
//
//sealed trait ServerLocalOutput[+Request]
////sealed trait ClientLocalOutput[+Response]
//
//object CommonLocalOutput {
//  case class Message[A](value: A) extends ServerLocalOutput[A] // with ClientLocalOutput[A]
//  // TODO: EOF / cancellation as a single or separate types?
//}


// ---- STATE ----

object CallState {
  // not a standalone state, used in unary states
  case class Running[F[_]](cancel: F[Unit]) extends AnyVal
  object Running {
    def spawn[F[_]](dispatcher: Dispatcher[F], block: F[Unit])(implicit F: Sync[F]): F[Running[F]] = {
      F.delay(dispatcher.unsafeRunCancelable(block)).map { (cancel: () => Future[Unit]) =>
        Running(F.delay(cancel()).void)
      }
    }
  }

  sealed trait ClientUnary[F[_], -Response]
  sealed trait ClientStream[F[_]]

  sealed trait ServerUnary[F[_], -Request, +Response]
//  sealed trait ServerStream[F[_]]

  case class Done[F[_]]() extends ClientUnary[F, Any] with ClientStream[F] with ServerUnary[F, Any, Nothing]

  case class PendingMessage[F[_], Response](handler: Either[Throwable, Response] => Unit) extends ClientUnary[F, Response]
  case class PendingCloseHandler[F[_], Response](handler: Either[Throwable, Response] => Unit, value: Response) extends ClientUnary[F, Any]
  case class PendingClose[F[_]](dummy: F[Unit]) extends ClientStream[F]

  case class Idle[F[_]]() extends ClientStream[F]
//  case class Completed[F[_]](dummy: F[Unit]) extends ClientState[F, Nothing, Any]

  // TODO these are a bit like the above, but with different accepting arguments. Can they be combined?
  case class PendingMessageServer[F[_], Request, Response](handler: Request => F[Running[F]]) extends ServerUnary[F, Request, Response]
  case class PendingHalfClose[F[_], Request, Response](handler: Request => F[Running[F]], request: Request) extends ServerUnary[F, Request, Response]
  case class Called[F[_]](running: Running[F]) extends ServerUnary[F, Any, Nothing]
}
