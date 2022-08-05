package fs2.grpc.shared

import cats.{Applicative, Monad}
import cats.effect.std.Dispatcher
import cats.effect.{Async, Deferred, Ref, SyncIO}
import cats.syntax.all._
import fs2.Stream
import fs2.grpc.internal.{ClientRemoteOutput, RemoteOutput, RemoteProxy}
import io.grpc.{ClientCall, ServerCall}

private[grpc] trait StreamOutput[F[_], T] {
  def onReady: SyncIO[Unit]

  def writeStream(s: Stream[F, T]): Stream[F, Unit]
}

private [grpc] object StreamOutput {
  def client[F[_], Request, Response](
    c: ClientCall[Request, Response],
    dispatcher: Dispatcher[F]
  )(implicit F: Async[F]): F[StreamOutput[F, Request]] = {
    Ref[F].of(Option.empty[Deferred[F, Unit]]).map { waiting =>
      new StreamOutputImpl[F, Request](waiting, dispatcher,
        isReady = F.delay(c.isReady),
        sendMessage = m => F.delay(c.sendMessage(m)))
    }
  }

  def server[F[_], Request, Response](
    c: ServerCall[Request, Response],
    dispatcher: Dispatcher[F]
  )(implicit F: Async[F]): F[StreamOutput[F, Response]] = {
    Ref[F].of(Option.empty[Deferred[F, Unit]]).map { waiting =>
      new StreamOutputImpl[F, Response](waiting, dispatcher,
        isReady = F.delay(c.isReady),
        sendMessage = m => F.delay(c.sendMessage(m)))
    }
  }
}

private[grpc] class StreamOutputImpl[F[_], T](
  waiting: Ref[F, Option[Deferred[F, Unit]]],
  dispatcher: Dispatcher[F],
  isReady: F[Boolean],
  sendMessage: T => F[Unit],
)(implicit F: Async[F]) extends StreamOutput[F, T] {
  override def onReady: SyncIO[Unit] = SyncIO.delay(dispatcher.unsafeRunAndForget(signal))

  private def signal: F[Unit] = waiting.getAndSet(None).flatMap {
    case None => F.unit
    case Some(wake) => wake.complete(()).void
  }

  override def writeStream(s: Stream[F, T]): Stream[F, Unit] = s.evalMap(sendWhenReady)

  private def sendWhenReady(msg: T): F[Unit] = {
    val send = sendMessage(msg)
    isReady.ifM(send, {
      Deferred[F, Unit].flatMap { wakeup =>
        waiting.set(wakeup.some) *>
          isReady.ifM(signal, F.unit) *> // trigger manually in case onReady was invoked before we installed wakeup
          wakeup.get *>
          send
      }
    })
  }
}

private[grpc] trait OnReadyListener[F[_]] {
  def onReady: F[Unit]
}
object OnReadyListener {
  def noop[F[_]](implicit F: Monad[F]): OnReadyListener[F] = new OnReadyListener[F] {
    override def onReady: F[Unit] = F.unit
  }
}


private[grpc] class StreamOutputImpl2[F[_], T](
  proxy: RemoteProxy[F, RemoteOutput.Message[T]],
  waiting: Ref[F, Option[Deferred[F, Unit]]],
)(implicit F: Async[F]) extends OnReadyListener[F] {
  def onReady: F[Unit] = waiting.getAndSet(None).flatMap {
    case None => F.unit
    case Some(wake) => wake.complete(()).void
  }

  def sendWhenReady(msg: T): F[Unit] = {
    val send = proxy.send(RemoteOutput.Message(msg))
    proxy.isReady.ifM(send, {
      Deferred[F, Unit].flatMap { wakeup =>
        waiting.set(wakeup.some) *>
          proxy.isReady.ifM(onReady, F.unit) *> // trigger manually in case onReady was invoked before we installed wakeup
          wakeup.get *>
          send
      }
    })
  }
}
private [grpc] object StreamOutput2 {
  def apply[F[_], T](
    proxy: RemoteProxy[F, RemoteOutput.Message[T]],
  )(implicit F: Async[F]): F[StreamOutputImpl2[F, T]] = {
    Ref[F].of(Option.empty[Deferred[F, Unit]]).map { waiting =>
      new StreamOutputImpl2[F, T](proxy, waiting)
    }
  }
}
