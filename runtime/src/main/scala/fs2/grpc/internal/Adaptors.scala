package fs2.grpc.internal

import cats.effect.kernel.Ref
import cats.effect.{Async, Sync}
import cats.syntax.all._
import cats.{Monad, MonadError}
import fs2.grpc.client.StreamIngest
import fs2.grpc.shared.{OnReadyListener, StreamOutputImpl2}
import io.grpc.{ClientCall, ServerCall, Status}


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
  )(implicit F: Sync[F]): RemoteProxy[F, RemoteOutput.Client[Request]]
  = new RemoteProxy[F, RemoteOutput.Client[Request]] {
    override def send(output: RemoteOutput.Client[Request]): F[Unit] = F.delay(output match {
      case RemoteOutput.Message(value) => call.sendMessage(value)
      case RemoteOutput.RequestMore(n) => call.request(n)
      case RemoteOutput.HalfClose => call.halfClose()
      case RemoteOutput.Cancel => call.cancel("call cancelled", null)
    })

    override def isReady: F[Boolean] = F.delay(call.isReady)
  }

  def server[F[_], Request, Response](
    call: ServerCall[Request, Response]
  )(implicit F: Sync[F]): RemoteProxy[F, RemoteOutput.Server[Response]]
  = new RemoteProxy[F, RemoteOutput.Server[Response]] {
    override def send(output: RemoteOutput.Server[Response]): F[Unit] = F.delay(output match {
      case RemoteOutput.Message(value) => call.sendMessage(value)
      case RemoteOutput.RequestMore(n) => call.request(n)
      case RemoteOutput.Error(status) => call.close(status, null)
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
  onReadyListener: OnReadyListener[F],
  ingest: StreamIngest[F, Response],
  state: Ref[F, CallState.ClientStream[F]],
)(implicit F: Async[F]) extends RemoteAdaptor[F, RemoteInput.Client[Response]] {
  override def onRemote(input: RemoteInput.Client[Response]): F[Unit] = {
    input match {
      case RemoteInput.Ready => onReadyListener.onReady
      case RemoteInput.Message(value) => ingest.onMessage(value)
      case RemoteInput.Close(status, trailers) =>
        (if (status.isOk) {
          state.get.flatMap {
            case _: CallState.PendingClose[_] => ingest.onClose(None)
            case other => ingest.onClose(Some(Status.INTERNAL
              .withDescription(s"Server closed call prematurely in state $other")
              .asRuntimeException(trailers)))
          }
        } else {
          ingest.onClose(Option.when(!status.isOk)(status.asRuntimeException(trailers)))
        }) *> state.set(CallState.Done())
    }
  }
}

class ClientLocalStreamAdaptor[F[_], Request](
  output: StreamOutputImpl2[F, Request],
  proxy: RemoteProxy[F, RemoteOutput.Client[Request]],
  state: Ref[F, CallState.ClientStream[F]],
)(implicit F: Async[F]) extends LocalAdaptor[F, LocalInput.Client[Request]] {
  override def onLocal(input: LocalInput.Client[Request]): F[Unit] = {
    input match {
      case LocalInput.Message(value) =>
        state.get.flatMap {
          case _: CallState.Idle[_] => output.sendWhenReady(value)
          case _: CallState.Done[_] => F.unit // call terminated, will be reported elsewhere
        }
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
      case LocalInput.Cancel => proxy.send(RemoteOutput.Cancel)
    }
  }
}


class ClientLocalUnaryAdaptor[F[_], Request](
  proxy: RemoteProxy[F, RemoteOutput.Client[Request]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends LocalAdaptor[F, LocalInput.Client[Request]] {
  override def onLocal(input: LocalInput.Client[Request]): F[Unit] = {
    input match {
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
      case LocalInput.Message(value) => proxy.send(RemoteOutput.Message(value))
      case LocalInput.Cancel => proxy.send(RemoteOutput.Cancel)
    }
  }
}

class ClientRemoteUnaryAdaptor[F[_], Request, Response](
  state: Ref[F, CallState.ClientUnary[F, Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable])
  extends RemoteAdaptor[F, RemoteInput.Client[Response]]
{
  override def onRemote(input: RemoteInput.Client[Response]): F[Unit] = {
    input match {
      case RemoteInput.Ready => F.unit // no backpressure for unary
      case RemoteInput.Message(value) =>
        state.modify {
          case CallState.PendingMessage(h) => (CallState.PendingCloseHandler(h, value), F.unit)
          case state: CallState.Done[F] => (state, F.unit)
        }.flatMap(identity)

      case RemoteInput.Close(status, trailers) =>
        state.modify {
          case CallState.PendingCloseHandler(h, value) => (CallState.Done(), h(Right(value)))
          case CallState.PendingMessage(h) => {
            val errorStatus = if (status.isOk) {
              Status.INTERNAL.withDescription("No message received before close")
            } else {
              status
            }
            (CallState.Done(), h(Left(errorStatus.asRuntimeException(trailers))))
          }
          case state @ CallState.Done() => (state, F.unit)
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
  proxy: RemoteProxy[F, RemoteOutput.Server[Response]],
  output: StreamOutputImpl2[F, Response],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
  extends LocalAdaptor[F, LocalInput.Server[Response]]
{
  override def onLocal(input: LocalInput.Server[Response]): F[Unit] = {
    input match {
      case LocalInput.Message(value) => output.sendWhenReady(value)
      case LocalInput.RequestMore(n) => proxy.send(RemoteOutput.RequestMore(n))
    }
  }
}

class ServerLocalUnaryAdaptor[F[_], Response](
  proxy: RemoteProxy[F, RemoteOutput.Server[Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F])
  extends LocalAdaptor[F, LocalInput.Server[Response]]
{
  override def onLocal(input: LocalInput.Server[Response]): F[Unit] = {
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
  extends RemoteAdaptor[F, RemoteInput.Server[Request]]
{
  override def onRemote(input: RemoteInput.Server[Request]): F[Unit] = {
    input match {
      case RemoteInput.Ready => output.onReady
      case RemoteInput.Message(value) => ingest.onMessage(value)
      case RemoteInput.HalfClose => ingest.onClose(None)
    }
  }
}

class ServerRemoteUnaryAdaptor[F[_], Request, Response](
  state: Ref[F, CallState.ServerUnary[F, Request, Response]],
  proxy: RemoteProxy[F, RemoteOutput.Server[Response]],
)(implicit F: Monad[F], FE: MonadError[F, Throwable], Async: Async[F], Sync: Sync[F])
  extends RemoteAdaptor[F, RemoteInput.Server[Request]]
{
  private def terminate(result: RemoteOutput.Server[Response]): F[Unit] =
    state.set(CallState.Done()) *> proxy.send(result)

  override def onRemote(input: RemoteInput.Server[Request]): F[Unit] = {
    input match {
      case RemoteInput.Cancel => state.modify {
        case CallState.Called(cancel) => (CallState.Done(), cancel.to[F])
      }.flatMap(identity)

      case RemoteInput.Message(request) => state.get.flatMap {
        case CallState.PendingMessageServer(handler) => state.set(CallState.PendingHalfClose(handler, request))
        case CallState.PendingHalfClose(_, _) => terminate(RemoteOutput.Error(
          Status.INTERNAL.withDescription("Too many requests")
        ))
        case _ => F.unit
      }
    }
  }
}