package fs2.grpc.internal

import cats.effect.SyncIO
import cats.effect.std.Dispatcher
import io.grpc.{ClientCall, Metadata, ServerCall, Status}


object Listener {
  def clientCall[F[_], Response](adaptor: RemoteAdaptor[F, RemoteInput.Client[Response]], dispatcher: Dispatcher[F]): ClientCall.Listener[Response] = {
    new ClientCall.Listener[Response] {
      private def emit(event: RemoteInput.Client[Response]): Unit = {
        dispatcher.unsafeRunSync(adaptor.onRemote(event))
      }

      override def onMessage(message: Response): Unit = {
        emit(RemoteInput.Message(message))
      }

      override def onClose(status: Status, trailers: Metadata): Unit = {
        emit(RemoteInput.Close(status, trailers))
      }

      override def onReady(): Unit = {
        emit(RemoteInput.Ready)
      }
    }
  }

  def serverCall[F[_], Request](adaptor: RemoteAdaptor[F, RemoteInput.Server[Request]], dispatcher: Dispatcher[F]): ClientCall.Listener[Request] = {
    new ServerCall.Listener[Request] {
      private def emit(event: RemoteInput.Server[Request]): Unit = {
        dispatcher.unsafeRunSync(adaptor.onRemote(event))
      }

      override def onMessage(message: Request): Unit = {
        emit(RemoteInput.Message(message))
      }

      override def onReady(): Unit = {
        emit(RemoteInput.Ready)
      }

      override def onHalfClose(): Unit = {
        emit(RemoteInput.HalfClose)
      }

      override def onCancel(): Unit = {
        emit(RemoteInput.Cancel)
      }

      override def onComplete(): Unit = {
        emit(RemoteInput.Complete)
      }
    }
  }
}
