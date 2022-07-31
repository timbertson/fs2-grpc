package fs2.grpc.internal

import cats.{Monad, MonadError}
import cats.effect.{Async, SyncIO}
import cats.effect.kernel.Ref
import cats.syntax.all._
import fs2.grpc.shared.StreamOutput
import io.grpc.{ClientCall, Metadata, Status}

//  ---- REMOTE INPUT ----

sealed trait ServerRemoteInput[+Request]
sealed trait ClientRemoteInput[+Response]

// inputs originating from the remote code (i.e. coming over the wire)
object CommonRemoteInput {
  case object Ready extends ServerRemoteInput[Nothing] with ClientRemoteInput[Nothing]
  case class Message[A](value: A) extends ServerRemoteInput[A] with ClientRemoteInput[A]
}

//  ---- LOCAL INPUT ----

sealed trait ServerLocalInput[+Response]
sealed trait ClientLocalInput[+Request]

// TODO different stream inputs / outputs?

// inputs originating from the local code (i.e. the application)
object CommonLocalInput {
  case class Message[A](value: A) extends ServerLocalInput[A] with ClientLocalInput[A]
  case class Terminate(exception: Option[Throwable]) extends ServerLocalInput[Nothing] with ClientLocalInput[Nothing]

  // client can cancel or terminate, server can only terminate
  case object Cancel extends ClientLocalInput[Nothing]
}

object ServerRemoteInput {
  case object HalfClose extends ServerRemoteInput[Any]
}

object ClientRemoteInput {
  case class Close(status: Status, trailers: Metadata) extends ClientRemoteInput[Nothing]
}

// ---- REMOTE OUTPUT ----
// outputs to be sent to the remote side

sealed trait ServerRemoteOutput[+Response]
sealed trait ClientRemoteOutput[+Request]

object CommonRemoteOutput {
  case class Message[A](value: A) extends ServerRemoteOutput[A] with ClientRemoteOutput[A]
}

sealed trait ServerRemoteOutput[-Response]
object ServerRemoteOutput {
  case class Error(status: Status) extends ServerRemoteOutput[Nothing]
}

object ClientRemoteOutput {
  case object HalfClose extends ClientRemoteOutput[Nothing]
  case object Cancel extends ClientRemoteOutput[Nothing]
}

// ---- LOCAL OUTPUT ----

sealed trait ServerLocalOutput[+Request]
sealed trait ClientLocalOutput[+Response]

object CommonLocalOutput {
  case class Message[A](value: A) extends ServerLocalOutput[A] with ClientLocalOutput[A]
  // TODO: EOF / cancellation as a single or separate types?
}


// ---- STATE ----
// TODO how many stream/ unary states do we need?

sealed trait ServerState
object ServerState {
  case object Initial extends ServerState
  case object PendingMessage extends ServerState
  case object PendingHalfClose extends ServerState
  case object Cancelled extends ServerState
  case object Completed extends ServerState
}

sealed trait ClientState[F[_], +Request, -Response]
sealed trait UnaryClientState[F[_], -Response]
sealed trait StreamClientState[F[_], +Request, -Response]

object ClientState {
//  case class Initial[F[_], Response](handler: Either[Status, Response] => F[Unit]) extends UnaryClientState[F, Nothing, Response]
  case class PendingMessage[F[_], Response](handler: Either[Status, Response] => F[Unit]) extends UnaryClientState[F, Response]
  case class Done[F[_]](dummy: F[Unit]) extends UnaryClientState[F, Any]

  case class WaitingToSend[F[_], Request](message: Request, cont: F[Unit]) extends ClientState[F, Request, Any]
  case class Idle[F[_]](dummy: F[Unit]) extends ClientState[F, Nothing, Any]
//  case class Completed[F[_]](dummy: F[Unit]) extends ClientState[F, Nothing, Any]
}


// -------

trait CallAdaptor[F[_], LI, RI, LO, RO] {
  def onRemote(input: RI): F[Unit]
  def onLocal(input: LI): F[Unit]
}

trait RemoteProxy[F[_], RO] {
  def send(output: RO): F[Unit]
  def isReady: F[Boolean]
}

class ClientUnaryToUnaryAdaptor[F[_], Request, Response](
  state: Ref[F, UnaryClientState[F, Response]],
  proxy: RemoteProxy[F, ClientRemoteOutput[Request]],
  )(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends CallAdaptor[
    F,
    ClientLocalInput[Request],
    ClientRemoteInput[Response],
    ClientLocalOutput[Response],
    ClientRemoteOutput[Request]]
{
  override def onRemote(input: ClientRemoteInput[Response]): F[Unit] = {
    input match {
      case CommonRemoteInput.Ready => F.unit // no backpressure for unary
      case CommonRemoteInput.Message(value) =>
        state.modify {
          case ClientState.PendingMessage(h) => (ClientState.Done(F.unit), h(Right(value)))
          case state: ClientState.Done[F] => (state, F.unit)
        }.flatMap(identity)

      case ClientRemoteInput.Close(status, trailers) =>
        // TODO trailers
        state.get.flatMap {
          case ClientState.PendingMessage(h) => h(Left(status))
          case done @ ClientState.Done[F] => F.unit
        }
    }
  }

  override def onLocal(input: ClientLocalInput[Request]): F[Unit] = {
    input match {
      case CommonLocalInput.Message(value) => ??? // do we bother? send out of bound maybe?
      case CommonLocalInput.Terminate(exception) => ??? // impossible
      case CommonLocalInput.Cancel => ??? // impossible
    }
  }
}

object Adaptor {
  def invalidInput[F[_]](input: Any, state: Any)(implicit FE: MonadError[F, Throwable]): F[Unit] = {
    FE.raiseError(new RuntimeException(s"Invalid input $input for state $state"))
  }
}

class ClientStreamToStreamAdaptor[F[_], Request, Response](
  state: Ref[F, ClientState[F, Request, Response]],
  proxy: RemoteProxy[F, ClientRemoteOutput[Request]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends CallAdaptor[
    F,
    ClientLocalInput[Request],
    ClientRemoteInput[Response],
    ClientLocalOutput[Response],
    ClientRemoteOutput[Request]]
{
  override def onRemote(input: ClientRemoteInput[Response]): F[Unit] = {
    Async
    input match {
      case CommonRemoteInput.Ready => state.modify {
        case ClientState.WaitingToSend(value, cont) => (ClientState.Idle(F.unit), proxy.send(CommonRemoteOutput.Message(value)) *> cont)
      }.flatMap(identity)

      case CommonRemoteInput.Message(value) =>
        state.modify {
          case ClientState.PendingMessage(h) => (ClientState.Completed(F.unit), h(Right(value)))
          case state => (state, Adaptor.invalidInput(input, state))
        }.flatMap(identity)

      case ClientRemoteInput.Close(status, trailers) =>
        // TODO trailers
        state.get.flatMap {
          case ClientState.PendingMessage(h) => h(Left(status))
          case _ => ???
        }
    }
  }

  override def onLocal(input: ClientLocalInput[Request]): F[Unit] = {
    input match {
      case CommonLocalInput.Message(value) =>
        state.modify {
          case ClientState.Initial(handler) => (ClientState.PendingMessage(handler), proxy.send(CommonRemoteOutput.Message(value)))
          case ClientState.PendingMessage(handler) => ???
          case ClientState.Completed(dummy) => ???
        }.flatMap(identity)
      case CommonLocalInput.Terminate(exception) => ??? // impossible
      case CommonLocalInput.Cancel => ??? // impossible
    }
  }
}

trait CallTransition[S, I, O] {
  def transition(state: S, input: I): (S, O)
}

class CallState[F[_], S, I, O](state: Ref[F, S], transition: CallTransition[S, I, O], action: O => F[Unit])(implicit F: Monad[F]) {
  def transition(i: I): F[Unit] =
    state.modify(state => transition.transition(state, i)).flatMap(action)
}

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

object Client {
  def mkListener[Request, Response](
    state: CallState[SyncIO, ClientState, ClientInput[Response], ClientOutput[Request]]
  ): ClientCall.Listener[Response] = {
    new ClientCall.Listener[Response] {
      override def onHeaders(headers: Metadata): Unit = {
        ???
      }

      override def onMessage(message: Response): Unit = {
        state.transition(CommonInput.Message(message)).unsafeRunSync()
      }

      override def onClose(status: Status, trailers: Metadata): Unit = {
        state.transition(ClientInput.Close(status, trailers)).unsafeRunSync()
      }

      override def onReady(): Unit = {
        state.transition(CommonInput.Ready).unsafeRunSync()
      }
    }
  }
}
