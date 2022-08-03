package fs2.grpc.internal
import cats.effect.implicits._
import cats.effect.kernel.Outcome
import fs2.Stream
import cats.effect.std.Dispatcher
import cats.effect.{Async, Ref, Resource, SyncIO}
import cats.syntax.all._
import fs2.grpc.client.ClientOptions
import fs2.grpc.shared.{StreamOutput2, StreamOutputImpl2}
import io.grpc._

final case class UnaryResult[A](value: Option[A], status: Option[GrpcStatus])
final case class GrpcStatus(status: Status, trailers: Metadata)

class ClientCallRunner[F[_], Request, Response] private[client] (
  call: ClientCall[Request, Response],
  dispatcher: Dispatcher[F],
  options: ClientOptions
)(implicit F: Async[F]) {
  val proxy = RemoteProxy.client[F, Request, Response](call)

  def unaryToUnaryCall(message: Request, headers: Metadata): F[Response] = {
    val local = new ClientLocalUnaryAdaptor(proxy)
    F.async[Response] { cb =>
      Ref[F].of[UnaryClientState[F, Response]](ClientState.PendingMessage(cb)).flatMap { ref =>
        val remote = new ClientRemoteUnaryAdaptor(ref)
        val listener = Listener.clientCall(remote, dispatcher)
        call.start(listener, headers)
        local.onLocal(LocalInput.RequestMore(1)) *>
          local.onLocal(LocalInput.Message(message)) *>
          local.onLocal(LocalInput.HalfClose) *>
          F.pure(local.onLocal(LocalInput.Cancel))
      }
    }
  }

  def streamingToUnaryCall(messages: Stream[F, Request], headers: Metadata): F[Response] = {
    val local = new ClientLocalUnaryAdaptor(proxy)

    // TODO can we compose StreamOutput to share the state ref?
    // Is it even a good idea? One is incoming, one outgoing?
    StreamOutput2[F, Request](proxy).flatMap { streamOutput =>
      F.async[Response] { cb =>
        Ref[F].of[UnaryClientState[F, Response]](ClientState.PendingMessage(cb)).flatMap { ref =>
          val remote = new ClientRemoteUnaryAdaptor(ref)
          call.start(Listener.clientCall(remote, dispatcher), headers)

          // Initially ask for two responses from flow-control so that if a misbehaving server
          // sends more than one responses, we can catch it and fail it in the listener.
          local.onLocal(LocalInput.RequestMore(2)) *>
            messages.evalMap(streamOutput.sendWhenReady).compile.drain.guaranteeCase {
              case Outcome.Succeeded(_) => local.onLocal(LocalInput.HalfClose)
              case Outcome.Errored(_) | Outcome.Canceled() => local.onLocal(LocalInput.Cancel)
            }.start.map(sendFiber => Some(sendFiber.cancel))
        }
      }
    }
  }

  def unaryToStreamingCall(message: Request, md: Metadata): Stream[F, Response] =
    Stream
      .resource(mkStreamListenerR(md, SyncIO.unit))
      .flatMap(Stream.exec(sendSingleMessage(message)) ++ _.stream.adaptError(ea))

//  def streamingToStreamingCall(messages: Stream[F, Request], md: Metadata): Stream[F, Response] = {
//    val local = new ClientLocalUnaryAdaptor(proxy)
//    Ref[F].of[StreamClientState[F]](ClientState.Idle[F]()).flatMap { stateRef =>
//      // TODO can we compose StreamOutput to share the above ref? Is it even a good idea? One is incoming, one outgoing?
//      StreamOutput2[F, Request](proxy).flatMap { streamOutput =>
//        val remote = new ClientRemoteUnaryAdaptor(stateRef)
//        val listener = Listener.clientCall(remote, dispatcher)
//        call.start(listener, headers)
//        local.onLocal(CommonLocalInput.RequestMore(1)) *>
//          local.onLocal(CommonLocalInput.Message(message)) *>
//          local.onLocal(CommonLocalInput.HalfClose) *>
//          F.pure(local.onLocal(CommonLocalInput.Cancel))
//      }
//    }

  //

  private def handleExitCase(cancelSucceed: Boolean): (ClientCall.Listener[Response], Resource.ExitCase) => F[Unit] = {
    case (_, Resource.ExitCase.Succeeded) => cancel("call done".some, None).whenA(cancelSucceed)
    case (_, Resource.ExitCase.Canceled) => cancel("call was cancelled".some, None)
    case (_, Resource.ExitCase.Errored(t)) => cancel(t.getMessage.some, t.some)
  }

  private def mkStreamListenerR(
    md: Metadata,
    remote: RemoteAdaptor[F, ClientRemoteInput[Response]],
    local: LocalAdaptor[F, ClientLocalInput[Request]]
  ): Resource[F, ClientCall.Listener[Response]] = {
    val prefetchN = options.prefetchN.max(1)
    val listener = Listener.clientCall[F, Response](remote, dispatcher)
    val acquire = F.delay(call.start(listener, md)) <* local.onLocal(LocalInput.RequestMore(prefetchN))

    // TODO provide reason?
    Resource.makeCase(acquire)(_ => local.onLocal(LocalInput.Cancel))
  }
}

object ClientCallRunner {

  def apply[F[_]]: PartiallyAppliedClientCall[F] = new PartiallyAppliedClientCall[F]

  class PartiallyAppliedClientCall[F[_]](val dummy: Boolean = false) extends AnyVal {

    def apply[Request, Response](
      channel: Channel,
      methodDescriptor: MethodDescriptor[Request, Response],
      dispatcher: Dispatcher[F],
      clientOptions: ClientOptions
    )(implicit F: Async[F]): F[ClientCallRunner[F, Request, Response]] = {
      F.delay(
        new ClientCallRunner(
          channel.newCall[Request, Response](methodDescriptor, clientOptions.callOptionsFn(CallOptions.DEFAULT)),
          dispatcher,
          clientOptions
        )
      )
    }

  }

}
