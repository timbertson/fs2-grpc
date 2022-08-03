package fs2.grpc.internal

import cats.effect.SyncIO
import cats.effect.std.Dispatcher
import io.grpc.{ClientCall, Metadata, ServerCall, Status}


object Listener {
  def clientCall[F[_], Response](adaptor: RemoteAdaptor[F, ClientRemoteInput[Response]], dispatcher: Dispatcher[F]): ClientCall.Listener[Response] = {
    new ClientCall.Listener[Response] {
      private def emit(event: ClientRemoteInput[Response]): Unit = {
        dispatcher.unsafeRunSync(adaptor.onRemote(event))
      }

      override def onMessage(message: Response): Unit = {
        emit(CommonRemoteInput.Message(message))
      }

      override def onClose(status: Status, trailers: Metadata): Unit = {
        emit(ClientRemoteInput.Close(status, trailers))
      }

      override def onReady(): Unit = {
        emit(CommonRemoteInput.Ready)
      }
    }
  }

  def serverCall[F[_], Request](adaptor: RemoteAdaptor[F, RemoteInput[Request]], dispatcher: Dispatcher[F]): ClientCall.Listener[Request] = {
    new ServerCall.Listener[Request] {
      private def emit(event: RemoteInput[Request]): Unit = {
        dispatcher.unsafeRunSync(adaptor.onRemote(event))
      }

      override def onMessage(message: Request): Unit = {
        emit(CommonRemoteInput.Message(message))
      }

      override def onReady(): Unit = {
        emit(CommonRemoteInput.Ready)
      }

      def onHalfClose(): Unit = {
        emit(RemoteInput.HalfClose)
      }

      def onCancel(): Unit = {
        emit(RemoteInput.Cancel)
      }

      def onComplete(): Unit = {
        emit(RemoteInput.Close)
      }
    }
  }
}
