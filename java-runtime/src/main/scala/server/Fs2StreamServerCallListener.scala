package org.lyranthe.fs2_grpc
package java_runtime
package server

import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, Effect}
import cats.implicits._
import io.grpc.ServerCall
import fs2.concurrent.Queue
import fs2._

class Fs2StreamServerCallListener[F[_], Request, Response] private (
    requestQ: Queue[F, Option[Request]],
    signalReadiness: F[Unit],
    val isCancelled: Deferred[F, Unit],
    val call: Fs2ServerCall[F, Request, Response]
)(implicit F: Effect[F])
    extends ServerCall.Listener[Request]
    with Fs2ServerCallListener[F, Stream[F, *], Request, Response] {

  override def onReady(): Unit = signalReadiness.unsafeRun()

  override def onCancel(): Unit = {
    isCancelled.complete(()).unsafeRun()
  }

  override def onMessage(message: Request): Unit = {
    call.call.request(1)
    requestQ.enqueue1(message.some).unsafeRun()
  }

  override def onHalfClose(): Unit = requestQ.enqueue1(none).unsafeRun()

  override def source: Stream[F, Request] =
    requestQ.dequeue.unNoneTerminate
}

object Fs2StreamServerCallListener {
  class PartialFs2StreamServerCallListener[F[_]](val dummy: Boolean = false) extends AnyVal {

    def apply[Request, Response](
        call: ServerCall[Request, Response],
        signalReadiness: F[Unit],
        options: ServerCallOptions = ServerCallOptions.default
    )(implicit
        F: ConcurrentEffect[F]
    ): F[Fs2StreamServerCallListener[F, Request, Response]] =
      for {
        inputQ <- Queue.unbounded[F, Option[Request]]
        isCancelled <- Deferred[F, Unit]
        serverCall <- Fs2ServerCall[F, Request, Response](call, options)
      } yield new Fs2StreamServerCallListener[F, Request, Response](inputQ, signalReadiness, isCancelled, serverCall)
  }

  def apply[F[_]] = new PartialFs2StreamServerCallListener[F]

}
