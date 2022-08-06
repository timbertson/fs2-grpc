package fs2.grpc.internal

import cats.syntax.all._
import cats.effect.{Async, Outcome, Ref, Sync, SyncIO}
import cats.effect.std.Dispatcher
import fs2.Stream
import fs2.grpc.internal.CallState.Running
import fs2.grpc.server.internal.{Fs2ServerCall, Fs2UnaryServerCallHandler}
import fs2.grpc.server.{Fs2StreamServerCallListener, ServerCallOptions, ServerOptions}
import fs2.grpc.shared.{OnReadyListener, StreamOutput}
import io.grpc.{Metadata, ServerCall, ServerCallHandler, Status, StatusException, StatusRuntimeException}

import scala.concurrent.Future

class ServerCallRunner[F[_]: Async] private (
  dispatcher: Dispatcher[F],
  options: ServerOptions
) {
  import ServerCallRunner.Handler

  def unaryToUnaryCall[Request, Response](
    implementation: (Request, Metadata) => F[Response]
  ): ServerCallHandler[Request, Response] =
    Handler.unary(implementation, options, dispatcher)

//  def unaryToStreamingCall[Request, Response](
//    implementation: (Request, Metadata) => Stream[F, Response]
//  ): ServerCallHandler[Request, Response] =
//    Fs2UnaryServerCallHandler.stream(implementation, options, dispatcher)
//
//  def streamingToUnaryCall[Request, Response](
//    implementation: (Stream[F, Request], Metadata) => F[Response]
//  ): ServerCallHandler[Request, Response] = new ServerCallHandler[Request, Response] {
//    def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
//      val listener = dispatcher.unsafeRunSync(Fs2StreamServerCallListener[F](call, SyncIO.unit, dispatcher, options))
//      listener.unsafeUnaryResponse(new Metadata(), implementation(_, headers))
//      listener
//    }
//  }
//
//  def streamingToStreamingCall[Request, Response](
//    implementation: (Stream[F, Request], Metadata) => Stream[F, Response]
//  ): ServerCallHandler[Request, Response] = new ServerCallHandler[Request, Response] {
//    def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
//      val (listener, streamOutput) = dispatcher.unsafeRunSync(StreamOutput.server(call, dispatcher).flatMap { output =>
//        Fs2StreamServerCallListener[F](call, output.onReady, dispatcher, options).map((_, output))
//      })
//      listener.unsafeStreamResponse(streamOutput, new Metadata(), implementation(_, headers))
//      listener
//    }
//  }

}
object ServerCallRunner {
  def apply[F[_]: Async](
    dispatcher: Dispatcher[F],
    serverOptions: ServerOptions
  ): ServerCallRunner[F] =
    new ServerCallRunner[F](dispatcher, serverOptions)

  object Handler {
    private def startCallF[F[_], Request, Response](
      call: ServerCall[Request, Response],
      onReadyListener: OnReadyListener[F],
      options: ServerCallOptions,
      f: RemoteProxy[F, RemoteOutput.Server[Response]] => Request => F[Running[F]]
    )
    (implicit F: Async[F])
    : F[RemoteAdaptor[F, RemoteInput.Server[Request]]] = {

      for {
        proxy <- RemoteProxy.server[F, Request, Response](options, call)
        // We expect only 1 request, but we ask for 2 requests here so that if a misbehaving client
        // sends more than 1 requests, ServerCall will catch it.
        _ <- proxy.send(RemoteOutput.RequestMore(2))
        val handleRequest: (Request => F[Running[F]]) = f(proxy)
        state <- Ref[F].of[CallState.ServerUnary[F, Request, Response]](CallState.PendingMessageServer[F, Request, Response](handleRequest))
      } yield new ServerRemoteUnaryAdaptor[F, Request, Response](state, proxy)
    }

    def unary[F[_]: Sync, Request, Response](
      impl: (Request, Metadata) => F[Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
    )(implicit F: Async[F]): ServerCallHandler[Request, Response] =
      new ServerCallHandler[Request, Response] {
        private val opt = options.callOptionsFn(ServerCallOptions.default)

        def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
          val fn = (proxy: RemoteProxy[F, RemoteOutput.Server[Response]]) => (request: Request) => {
            val respond = impl(request, headers).flatMap { response =>
              proxy.send(RemoteOutput.Message(response))
            }
            spawn(proxy, dispatcher, respond)
          }
          dispatcher.unsafeRunSync(startCallF[F, Request, Response](call, ???, opt, fn).map { adaptor =>
            Listener.serverCall(adaptor, dispatcher)
          })
        }
      }

    private def spawn[F[_], Response](
      proxy: RemoteProxy[F, RemoteOutput.Server[Response]],
      dispatcher: Dispatcher[F],
      block: F[Unit],
    )(implicit F: Sync[F]): F[Running[F]] = {
      val bracketed = F.guaranteeCase(block)(outcome => proxy.send(response(outcome)))
      Running.spawn(dispatcher, bracketed)
    }

    private def response[F[_], Response](outcome: Outcome[F, Throwable, Unit]): RemoteOutput.Server[Nothing] = {
      outcome match {
        case Outcome.Succeeded(_) => RemoteOutput.Close(Status.OK)
        case Outcome.Errored(e) => closeWithError(e)
        case Outcome.Canceled() => RemoteOutput.Close(Status.CANCELLED)
      }
    }

    private def closeWithError(t: Throwable): RemoteOutput.Close = t match {
      case ex: StatusException => RemoteOutput.Close(ex.getStatus, Option(ex.getTrailers))
      case ex: StatusRuntimeException => RemoteOutput.Close(ex.getStatus, Option(ex.getTrailers))
      case ex => RemoteOutput.Close(Status.INTERNAL.withDescription(ex.getMessage).withCause(ex))
    }
  }
}
