package fs2.grpc.internal
import cats.effect.implicits._
import cats.effect.kernel.Outcome
import cats.effect.std.Dispatcher
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._
import fs2.Stream
import fs2.grpc.client.{ClientOptions, StreamIngest}
import fs2.grpc.shared.{OnReadyListener, StreamOutput2}
import io.grpc._

final case class UnaryResult[A](value: Option[A], status: Option[GrpcStatus])
final case class GrpcStatus(status: Status, trailers: Metadata)

class ClientCallRunner[F[_], Request, Response] (
  call: ClientCall[Request, Response],
  dispatcher: Dispatcher[F],
  options: ClientOptions
)(implicit F: Async[F]) {
  type Cancel = F[Unit]
  val proxy = RemoteProxy.client[F, Request, Response](call)

  def unaryToUnaryCall(message: Request, headers: Metadata): F[Response] = {
    anyToUnaryCall(headers) {
      proxy.send(RemoteOutput.RequestMore(1)) *>
        proxy.send(RemoteOutput.Message(message)) *>
        proxy.send(RemoteOutput.HalfClose) *>
        F.pure(proxy.send(RemoteOutput.Cancel))
    }
  }

  def streamingToUnaryCall(messages: Stream[F, Request], headers: Metadata): F[Response] = {
    anyToUnaryCall(headers) {
      StreamOutput2[F, Request](proxy).flatMap { streamOutput =>
        proxy.send(RemoteOutput.RequestMore(2)) *>
          messages.evalMap(streamOutput.sendWhenReady).compile.drain.guaranteeCase {
            case Outcome.Succeeded(_) => proxy.send(RemoteOutput.HalfClose)
            case Outcome.Errored(_) | Outcome.Canceled() => proxy.send(RemoteOutput.Cancel)
          }.start.map(_.cancel)
      }
    }
  }

  def unaryToStreamingCall(message: Request, md: Metadata): Stream[F, Response] = {
    val local = new ClientLocalAdaptor[F, Request](proxy)
    Stream
      .resource(anyToStreamResponse(md, local, OnReadyListener.noop[F]))
      .flatMap { response =>
        Stream.exec(proxy.send(RemoteOutput.Message(message))) ++ response
      }
  }

  def streamingToStreamingCall(messages: Stream[F, Request], md: Metadata): Stream[F, Response] = {
    val local = new ClientLocalAdaptor[F, Request](proxy)

    val resource = for {
      output <- Resource.eval(StreamOutput2[F, Request](proxy))
      sendAll = messages.evalMap(output.sendWhenReady) ++ Stream.eval(proxy.send(RemoteOutput.HalfClose))
      response <- anyToStreamResponse(md, local, output)
    } yield response.concurrently(sendAll)

    Stream
      .resource(resource)
      .flatMap(identity)
  }

  //

  private def anyToUnaryCall(headers: Metadata)(spawn: F[Cancel]): F[Response] = {
    F.async[Response] { cb =>
      Ref[F].of[CallState.ClientUnary[F, Response]](CallState.PendingMessage(cb)).flatMap { ref =>
        val remote = new ClientRemoteUnaryAdaptor(ref)
        val listener = Listener.clientCall(remote, dispatcher)
        call.start(listener, headers)
        spawn.map(Some.apply)
      }
    }
  }

  private def anyToStreamResponse[Listener<:OnReadyListener[F]](
    md: Metadata,
    local: LocalAdaptor[F, LocalInput.Client[Request]],
    onReadyListener: OnReadyListener[F],
  ): Resource[F, Stream[F, Response]] = {
    val prefetchN = options.prefetchN.max(1)
    val acquire = for {
      ref <- Ref[F].of[CallState.ClientStream[F]](CallState.Idle())
      ingest <- StreamIngest[F, Response](n => local.onLocal(LocalInput.RequestMore(n)), options.prefetchN)
      remote = new ClientRemoteStreamAdaptor(onReadyListener, ingest, ref)
      listener = Listener.clientCall[F, Response](remote, dispatcher)
      _ <- F.delay(call.start(listener, md))
      _ <- local.onLocal(LocalInput.RequestMore(prefetchN))
    } yield ingest.messages

    // TODO provide reason for cancellation?
    Resource.make(acquire)(_ => local.onLocal(LocalInput.Cancel))
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
