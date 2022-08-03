package fs2.grpc.internal

import cats.{Monad, MonadError}
import cats.syntax.all._
import cats.effect.{Async, Sync}
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import fs2.grpc.client.StreamIngest
import fs2.grpc.shared.StreamOutputImpl2
import io.grpc.{ClientCall, Metadata, ServerCall, Status}


trait RemoteAdaptor[F[_], R] {
  def onRemote(input: R): F[Unit]
}



trait LocalAdaptor[F[_], LI] {
  def onLocal(input: LI): F[Unit]
}

//trait CallAdaptor[F[_], LI, RI] {
//  def onRemote(input: RI): F[Unit]
//  def onLocal(input: LI): F[Unit]
//}

trait RemoteProxy[F[_], -RO] {
  def send(output: RO): F[Unit]
  def isReady: F[Boolean]
}

object RemoteProxy {
  def client[F[_], Request, Response](
    call: ClientCall[Request, Response]
  )(implicit F: Sync[F]): RemoteProxy[F, ClientRemoteOutput[Request]]
  = new RemoteProxy[F, ClientRemoteOutput[Request]] {
    override def send(output: ClientRemoteOutput[Request]): F[Unit] = F.delay(output match {
      case RemoteOutput.Message(value) => call.sendMessage(value)
      case RemoteOutput.RequestMore(n) => call.request(n)
      case ClientRemoteOutput.HalfClose => call.halfClose()
      case ClientRemoteOutput.Cancel => call.cancel("call cancelled", null)
    })

    override def isReady: F[Boolean] = F.delay(call.isReady)
  }

  def server[F[_], Request, Response](
    call: ServerCall[Request, Response]
  )(implicit F: Sync[F]): RemoteProxy[F, ServerRemoteOutput[Response]]
  = new RemoteProxy[F, ServerRemoteOutput[Response]] {
    override def send(output: ServerRemoteOutput[Response]): F[Unit] = F.delay(output match {
      case RemoteOutput.Message(value) => call.sendMessage(value)
      case RemoteOutput.RequestMore(n) => call.request(n)
      case ServerRemoteOutput.Error(status) => call.close(status, null)
    })

    override def isReady: F[Boolean] = F.delay(call.isReady)
  }
}

object Adaptor {
  def invalidInput[F[_]](input: Any, state: Any)(implicit FE: MonadError[F, Throwable]): F[Unit] = {
    FE.raiseError(new RuntimeException(s"Invalid input $input for state $state"))
  }
}

class ClientRemoteStreamAdaptor[F[_], Request, Response](
  output: StreamOutputImpl2[F, Response],
  ingest: StreamIngest[F, Response],
  state: Ref[F, StreamClientState[F]],
)(implicit F: Async[F]) extends RemoteAdaptor[F, ClientRemoteInput[Response]] {
  override def onRemote(input: ClientRemoteInput[Response]): F[Unit] = {
    input match {
      case CommonRemoteInput.Ready => output.onReady
      case CommonRemoteInput.Message(value) => ingest.onMessage(value)
      case ClientRemoteInput.Close(status, trailers) =>
        (if (status.isOk) {
          state.get.flatMap {
            case _: ClientState.PendingClose[_] => ingest.onClose(None)
            case other => ingest.onClose(Some(Status.INTERNAL
              .withDescription(s"Server closed call prematurely in state $other")
              .asRuntimeException(trailers)))
          }
        } else {
          ingest.onClose(Option.when(!status.isOk)(status.asRuntimeException(trailers)))
        }) *> state.set(CommonState.Done())
    }
  }
}

class ClientLocalStreamAdaptor[F[_], Request](
  output: StreamOutputImpl2[F, Request],
  proxy: RemoteProxy[F, ClientRemoteOutput[Request]],
  state: Ref[F, StreamClientState[F]],
)(implicit F: Async[F]) extends LocalAdaptor[F, ClientLocalInput[Request]] {
  override def onLocal(input: ClientLocalInput[Request]): F[Unit] = {
    input match {
      case LocalInput.Message(value) =>
        state.get.flatMap {
          case _: ClientState.Idle[_] => output.sendWhenReady(value)
          case _: CommonState.Done[_] => F.unit // call terminated, will be reported elsewhere
        }
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
      case LocalInput.Cancel => proxy.send(ClientRemoteOutput.Cancel)
    }
  }
}


class ClientLocalUnaryAdaptor[F[_], Request](
  proxy: RemoteProxy[F, ClientRemoteOutput[Request]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends LocalAdaptor[F, ClientLocalInput[Request]] {
  override def onLocal(input: ClientLocalInput[Request]): F[Unit] = {
    input match {
      case LocalInput.Message(value) => proxy.send(RemoteOutput.Message(value))
      case LocalInput.Cancel => proxy.send(ClientRemoteOutput.Cancel)
    }
  }
}

class ClientRemoteUnaryAdaptor[F[_], Request, Response](
  state: Ref[F, UnaryClientState[F, Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends RemoteAdaptor[F, ClientRemoteInput[Response]]
{
  override def onRemote(input: ClientRemoteInput[Response]): F[Unit] = {
    input match {
      case CommonRemoteInput.Ready => F.unit // no backpressure for unary
      case CommonRemoteInput.Message(value) =>
        state.modify {
          case ClientState.PendingMessage(h) => (ClientState.PendingCloseHandler(h, value), F.unit)
          case state: CommonState.Done[F] => (state, F.unit)
        }.flatMap(identity)

      case ClientRemoteInput.Close(status, trailers) =>
        state.modify {
          case ClientState.PendingCloseHandler(h, value) => (CommonState.Done(), h(Right(value)))
          case ClientState.PendingMessage(h) => {
            val errorStatus = if (status.isOk) {
              Status.INTERNAL.withDescription("No message received before close")
            } else {
              status
            }
            (CommonState.Done(), h(Left(errorStatus.asRuntimeException(trailers))))
          }
          case state @ CommonState.Done() => (state, F.unit)
        }.flatMap(identity)
    }
  }
}

// onRemote cares about the current state, to see if the remote is acting well
//  - often delegates to streamInput
// onLocal doesn't care as much about state
//  - unary doesn't have anything
//  - depends on StreamOutput

class ServerLocalStreamAdaptor[F[_], Response](
  proxy: RemoteProxy[F, ServerRemoteOutput[Response]],
  output: StreamOutputImpl2[F, Response],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
  extends LocalAdaptor[F, ServerLocalInput[Response]]
{
  override def onLocal(input: ServerLocalInput[Response]): F[Unit] = {
    input match {
      case LocalInput.Message(value) => output.sendWhenReady(value)
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
    }
  }
}

class ServerLocalUnaryAdaptor[F[_], Response](
  proxy: RemoteProxy[F, ServerRemoteOutput[Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
  extends LocalAdaptor[F, ServerLocalInput[Response]]
{
  override def onLocal(input: ServerLocalInput[Response]): F[Unit] = {
    input match {
      case LocalInput.Message(value) => proxy.send(RemoteOutput.Message(value))
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
    }
  }
}


class ServerRemoteStreamAdaptor[F[_], Request, Response](
  ingest: StreamIngest[F, Request],
  output: StreamOutputImpl2[F, Response],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
  extends RemoteAdaptor[F, RemoteInput[Request]]
{
  override def onRemote(input: RemoteInput[Request]): F[Unit] = {
    input match {
      case CommonRemoteInput.Ready => output.onReady
      case CommonRemoteInput.Message(value) => ingest.onMessage(value)
      case RemoteInput.HalfClose => ingest.onClose(None)
    }
  }
}

class ServerRemoteUnaryAdaptor[F[_], Request, Response](
  state: Ref[F, UnaryServerState[F, Request, Response]],
  proxy: RemoteProxy[F, ServerRemoteOutput[Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F], Sync: Sync[F])
  extends RemoteAdaptor[F, RemoteInput[Request]]
{
  private def terminate(result: ServerRemoteOutput[Response]): F[Unit] =
    state.set(CommonState.Done()) *> proxy.send(result)

  override def onRemote(input: RemoteInput[Request]): F[Unit] = {
    input match {
      case RemoteInput.Cancel => state.modify {
        case ServerState.Called(cancel) => (CommonState.Done(), cancel.to[F])
      }.flatMap(identity)

      case CommonRemoteInput.Message(request) => state.get.flatMap {
        case ServerState.PendingMessage(handler) => state.set(ServerState.PendingHalfClose(handler, request))
        case ServerState.PendingHalfClose(_, _) => terminate(ServerRemoteOutput.Error(
          Status.INTERNAL.withDescription("Too many requests")
        ))
        case _ => F.unit
      }
    }
  }
}