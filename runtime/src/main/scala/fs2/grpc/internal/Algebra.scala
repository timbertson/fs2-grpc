package fs2.grpc.internal

import cats.effect.SyncIO
import io.grpc.{Metadata, Status}

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

  case class Message[A](value: A) extends Server[A] with Client[A]
  case class RequestMore(n: Int) extends Server[Nothing] with Client[Nothing]
  case object HalfClose extends Client[Nothing]

  // client can cancel or terminate, server can only terminate
  case object Cancel extends Client[Nothing]
}


// ---- REMOTE OUTPUT ----
// outputs to be sent to the remote side


object RemoteOutput {
  sealed trait Server[+Response]
  sealed trait Client[+Request]

  case class Message[A](value: A) extends Server[A] with Client[A]
  case class RequestMore(n: Int) extends Server[Nothing] with Client[Nothing]
  case class Error(status: Status) extends Server[Nothing]
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
  case class PendingMessageServer[F[_], Request, Response](handler: Request => F[Response]) extends ServerUnary[F, Request, Response]
  case class PendingHalfClose[F[_], Request, Response](handler: Request => F[Response], request: Request) extends ServerUnary[F, Request, Response]
  case class Called[F[_]](cancel: SyncIO[Unit]) extends ServerUnary[F, Any, Nothing]
}

//trait Processor[F[_], I] {
//  def apply(input: I): F[Unit]
//}
//trait StateProcessor[F[_], S, I] {
//  def apply(state: S, input: I): (S, F[Unit])
//}
//
//case class CompositeState[+Local, +Remote](local: Local, remote: Remote)
//
//class ClientReceiveStream[F[_], Response](
//
//) extends StateProcessor[F, CallState.ClientStream[F], RemoteInput.Client[Response]] {
//  override def apply(state: CallState.ClientStream[F], input: RemoteInput.Client[Response]): (CallState.ClientStream[F], F[Unit]) = {
//  }
//}

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
