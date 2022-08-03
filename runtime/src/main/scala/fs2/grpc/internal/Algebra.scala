package fs2.grpc.internal

import cats.{Monad, MonadError}
import cats.effect.{Async, Sync, SyncIO, kernel}
import cats.effect.kernel.{Concurrent, Ref}
import cats.syntax.all._
import fs2.grpc.client.StreamIngest
import fs2.grpc.server.internal.Fs2ServerCall.Cancel
import fs2.grpc.shared.{StreamOutput, StreamOutputImpl2}
import io.grpc.{ClientCall, Metadata, Status, StatusRuntimeException}

//  ---- REMOTE INPUT ----

sealed trait ServerRemoteInput[+Request]
sealed trait ClientRemoteInput[+Response]

// ---- REMOTE INPUT ----
// inputs originating from the remote code (i.e. coming over the wire)
object RemoteInput {
  case object Ready extends ServerRemoteInput[Nothing] with ClientRemoteInput[Nothing]
  case class Message[A](value: A) extends ServerRemoteInput[A] with ClientRemoteInput[A]
  case object HalfClose extends ServerRemoteInput[Nothing]
  case object Cancel extends ServerRemoteInput[Nothing]
  case class Close(status: Status, trailers: Metadata) extends ClientRemoteInput[Nothing]
}


//  ---- LOCAL INPUT ----
// inputs originating from the local code (i.e. the application)

sealed trait ServerLocalInput[+Response]
sealed trait ClientLocalInput[+Request]

// TODO different stream inputs / outputs?

object LocalInput {
  case class Message[A](value: A) extends ServerLocalInput[A] with ClientLocalInput[A]
  case class RequestMore(n: Int) extends ServerLocalInput[Nothing] with ClientLocalInput[Nothing]
  case object HalfClose extends ClientLocalInput[Nothing]

  // client can cancel or terminate, server can only terminate
  case object Cancel extends ClientLocalInput[Nothing]
}


// ---- REMOTE OUTPUT ----
// outputs to be sent to the remote side

sealed trait ServerRemoteOutput[+Response]
sealed trait ClientRemoteOutput[+Request]

object RemoteOutput {
  case class Message[A](value: A) extends ServerRemoteOutput[A] with ClientRemoteOutput[A]
  case class RequestMore(n: Int) extends ServerRemoteOutput[Nothing] with ClientRemoteOutput[Nothing]
  case class Error(status: Status) extends ServerRemoteOutput[Nothing]
  case object HalfClose extends ClientRemoteOutput[Nothing]
  case object Cancel extends ClientRemoteOutput[Nothing]
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
// TODO how many stream/ unary states do we need?

sealed trait UnaryClientState[F[_], -Response]
sealed trait StreamClientState[F[_]]

object CommonState {
  case class Done[F[_]]() extends UnaryClientState[F, Any] with StreamClientState[F] with UnaryServerState[F, Any, Nothing]
}

object ClientState {
//  case class Initial[F[_], Response](handler: Either[Status, Response] => F[Unit]) extends UnaryClientState[F, Nothing, Response]
//  def initialUnary[F[_], Response](implicit F: Async[F]): F[PendingMessage[F, Response]] =
//    F.async[Response]{ cb =>
//      Ref[F].of(PendingMessage[F, Response]({
//        case r: Right[Throwable, Response] => cb(r)
//        case l: Left[Throwable, Response] => cb(l)
//      })).map { ref =>
//        call.start(???, ???)
//        call.request(1)
//        call.sendMessage(message)
//        call.halfClose()
//        Some(onCancel(call))
//      }
//    }

//  def initialUnary[F[_], Response](cm: F[PendingMessage[F, Response]] =
//    F.async[Response]{ cb =>
//      Ref[F].of(PendingMessage[F, Response]({
//        case r: Right[Throwable, Response] => cb(r)
//        case l: Left[Throwable, Response] => cb(l)
//      })).map { ref =>
//        call.start(???, ???)
//        call.request(1)
//        call.sendMessage(message)
//        call.halfClose()
//        Some(onCancel(call))
//      }
//    }
  case class PendingMessage[F[_], Response](handler: Either[Throwable, Response] => Unit) extends UnaryClientState[F, Response]
  case class PendingCloseHandler[F[_], Response](handler: Either[Throwable, Response] => Unit, value: Response) extends UnaryClientState[F, Any]
  case class PendingClose[F[_]](dummy: F[Unit]) extends StreamClientState[F]

  case class Idle[F[_]]() extends StreamClientState[F]
//  case class Completed[F[_]](dummy: F[Unit]) extends ClientState[F, Nothing, Any]
}

sealed trait UnaryServerState[F[_], -Request, +Response]
sealed trait StreamServerState[F[_]]

object ServerState {
  // TODO these are a bit like the above, but with different accepting arguments. Can they be combined?
  case class PendingMessage[F[_], Request, Response](handler: Request => F[Response]) extends UnaryServerState[F, Request, Response]
  case class PendingHalfClose[F[_], Request, Response](handler: Request => F[Response], request: Request) extends UnaryServerState[F, Request, Response]
  case class Called[F[_]](cancel: Cancel) extends UnaryServerState[F, Any, Nothing]
}

trait Processor[F[_], I] {
  def apply(input: I): F[Unit]
}
trait StateProcessor[F[_], S, I] {
  def apply(state: S, input: I): (S, F[Unit])
}

case class CompositeState[+Local, +Remote](local: Local, remote: Remote)

class ClientReceiveStream[F[_], Response](

) extends StateProcessor[F, StreamClientState[F], ClientRemoteInput[Response]] {
  override def apply(state: StreamClientState[F], input: ClientRemoteInput): (StreamClientState[F], F[Unit]) = {
  }
}

// -------

//trait CallTransition[S, I, O] {
//  def transition(state: S, input: I): (S, O)
//}
//
//class CallState[F[_], S, I, O](state: Ref[F, S], transition: CallTransition[S, I, O], action: O => F[Unit])(implicit F: Monad[F]) {
//  def transition(i: I): F[Unit] =
//    state.modify(state => transition.transition(state, i)).flatMap(action)
//}

//class ClientUnaryTransition[Request, Response] extends CallTransition[ClientState, ClientInput[Response], ClientOutput[Request]] {
//  override def transition(state: ClientState, input: ClientInput[Response]): (ClientState, ClientOutput[Request]) = {
//    input match {
//      case ClientInput.Close(status, trailers) => ???
//      case input: CommonInput => ???
//      case CommonInput.Message(value) => ???
//    }
//  }
//}

//class ClientUnaryHandler[Response] {
//  def handle(output: ClientOutput[R
//}

//class ClientUnaryToUnaryAdaptor[F[_], Request, Response](
//  state: Ref[F, UnaryClientState[F, Response]],
//  proxy: RemoteProxy[F, ClientRemoteOutput[Request]],
//)(implicit F: Monad[F], FE: MonadError[F, Throwable])
//  extends CallAdaptor[
//    F,
//    ClientLocalInput[Request],
//    ClientRemoteInput[Response]]
//{
//  override def onRemote(input: ClientRemoteInput[Response]): F[Unit] = {
//    input match {
//      case CommonRemoteInput.Ready => F.unit // no backpressure for unary
//      case CommonRemoteInput.Message(value) =>
//        state.modify {
//          case ClientState.PendingMessage(h) => (ClientState.Done(F.unit), h(Right(value)))
//          case state: ClientState.Done[F] => (state, F.unit)
//        }.flatMap(identity)
//
//      case ClientRemoteInput.Close(status, trailers) =>
//        // TODO trailers
//        state.get.flatMap {
//          case ClientState.PendingMessage(h) => h(Left(status))
//          case done @ ClientState.Done[F] => F.unit
//        }
//    }
//  }
//
//  override def onLocal(input: ClientLocalInput[Request]): F[Unit] = {
//    input match {
//      case CommonLocalInput.Message(value) => ??? // do we bother? send out of bound maybe?
//      case CommonLocalInput.Cancel => ??? // impossible
//    }
//  }
//}
//class ServerStreamToStreamAdaptor[F[_], Request, Response](
//  proxy: RemoteProxy[F, ServerRemoteOutput[Response]],
//)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
//  extends CallAdaptor[
//    F,
//    ServerLocalInput[Response],
//    ServerRemoteInput[Request]]
//{
//  private val ingest: StreamIngest[F, Request] = ???
//  private val output: StreamOutputImpl2[F, Response] = ???
//
//  override def onRemote(input: ServerRemoteInput[Request]): F[Unit] = {
//    input match {
//      case CommonRemoteInput.Ready => output.onReady
//      case CommonRemoteInput.Message(value) => ingest.onMessage(value)
//      case ServerRemoteInput.HalfClose => ingest.onClose(None)
//    }
//  }
//
//  override def onLocal(input: ServerLocalInput[Response]): F[Unit] = {
//    input match {
//      case CommonLocalInput.Message(value) => output.sendWhenReady(value)
//      case CommonLocalInput.RequestMore(n) => proxy.send(CommonRemoteOutput.RequestMore(n))
//    }
//  }
//}
