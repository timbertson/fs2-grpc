package fs2.grpc.internal
import cats.effect.implicits._
import cats.effect.kernel.Outcome
import fs2.Stream
import cats.effect.std.Dispatcher
import cats.effect.{Async, Ref, Resource, SyncIO}
import cats.syntax.all._
import fs2.grpc.client.{ClientOptions, StreamIngest}
import fs2.grpc.shared.{OnReadyListener, StreamOutput2, StreamOutputImpl2}
import io.grpc._

final case class UnaryResult[A](value: Option[A], status: Option[GrpcStatus])
final case class GrpcStatus(status: Status, trailers: Metadata)

class ClientCallRunner[F[_], Request, Response] (
  call: ClientCall[Request, Response],
  dispatcher: Dispatcher[F],
  options: ClientOptions
)(implicit F: Async[F]) {
  val proxy = RemoteProxy.client[F, Request, Response](call)

  def unaryToUnaryCall(message: Request, headers: Metadata): F[Response] = {
    val local = new ClientLocalUnaryAdaptor(proxy)
    F.async[Response] { cb =>
      Ref[F].of[CallState.ClientUnary[F, Response]](CallState.PendingMessage(cb)).flatMap { ref =>
        val remote = new ClientRemoteUnaryAdaptor(ref)
        val listener = Listener.clientCall(remote, dispatcher)
        call.start(listener, headers)
        local.onLocal(LocalInput.RequestMore(1)) *>
          local.onLocal(LocalInput.Message(message)) *>
          local.onLocal(LocalInput.HalfClose) *>
          F.pure(Some(local.onLocal(LocalInput.Cancel)))
      }
    }
  }

  def streamingToUnaryCall(messages: Stream[F, Request], headers: Metadata): F[Response] = {
    val local = new ClientLocalUnaryAdaptor(proxy)

    // TODO can we compose StreamOutput to share the state ref?
    // Is it even a good idea? One is incoming, one outgoing?
    StreamOutput2[F, Request](proxy).flatMap { streamOutput =>
      F.async[Response] { cb =>
        Ref[F].of[CallState.ClientUnary[F, Response]](CallState.PendingMessage(cb)).flatMap { ref =>
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

  def unaryToStreamingCall(message: Request, md: Metadata): Stream[F, Response] = {
    val local = new ClientLocalUnaryAdaptor[F, Request](proxy)
    val resource = for {
      ref <- Resource.eval(Ref[F].of[CallState.ClientStream[F]](CallState.Idle()))
      ingest <- Resource.eval(StreamIngest[F, Response](n => local.onLocal(LocalInput.RequestMore(n)), options.prefetchN))
      remote = new ClientRemoteStreamAdaptor(OnReadyListener.noop[F], ingest, ref)
      _ <- streamListen(md, remote, local)
    } yield ingest.messages
    Stream
      .resource(resource)
      .flatMap { response =>
        Stream.exec(local.onLocal(LocalInput.Message(message))) ++ response
      }
  }

  def streamingToStreamingCall(messages: Stream[F, Request], md: Metadata): Stream[F, Response] = {
    val local = new ClientLocalUnaryAdaptor[F, Request](proxy)

    val resource = for {
      // TODO can we compose StreamOutput to share the above ref? Is it even a good idea? One is incoming, one outgoing?
      ref <- Resource.eval(Ref[F].of[CallState.ClientStream[F]](CallState.Idle()))
      output <- Resource.eval(StreamOutput2[F, Request](proxy))
      ingest <- Resource.eval(StreamIngest[F, Response](n => local.onLocal(LocalInput.RequestMore(n)), options.prefetchN))
      remote = new ClientRemoteStreamAdaptor(OnReadyListener.noop[F], ingest, ref)
      _ <- streamListen(md, remote, local)
    } yield (output, ingest.messages)

    Stream
      .resource(resource)
      .flatMap { case (output, response) =>
        response.concurrently(messages.evalMap(output.sendWhenReady) ++ Stream.eval(local.onLocal(LocalInput.HalfClose))
      }
  }

  //

  private def streamListen(
    md: Metadata,
    remote: RemoteAdaptor[F, RemoteInput.Client[Response]],
    local: LocalAdaptor[F, LocalInput.Client[Request]]
  ): Resource[F, Unit] = {
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
