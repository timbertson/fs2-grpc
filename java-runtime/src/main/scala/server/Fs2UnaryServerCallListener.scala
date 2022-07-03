package org.lyranthe.fs2_grpc
package java_runtime
package server

import cats.effect.{ConcurrentEffect, Effect}
import cats.effect.concurrent.{Deferred, Ref}
import cats.syntax.all._
import io.grpc._

class Fs2UnaryServerCallListener[F[_], Request, Response] private (
    request: Ref[F, Option[Request]],
    signalReadiness: F[Unit],
    isComplete: Deferred[F, Unit],
    val isCancelled: Deferred[F, Unit],
    val call: Fs2ServerCall[F, Request, Response]
)(implicit F: Effect[F])
    extends ServerCall.Listener[Request]
    with Fs2ServerCallListener[F, F, Request, Response] {

  import Fs2UnaryServerCallListener._

  override def onReady(): Unit = signalReadiness.unsafeRun()

  override def onCancel(): Unit = {
    isCancelled.complete(()).unsafeRun()
  }

  override def onMessage(message: Request): Unit = {
    request.access
      .flatMap[Unit] { case (curValue, modify) =>
        if (curValue.isDefined)
          F.raiseError(statusException(TooManyRequests))
        else
          modify(message.some).void
      }
      .unsafeRun()

  }

  override def onHalfClose(): Unit =
    isComplete.complete(()).unsafeRun()

  override def source: F[Request] =
    for {
      _ <- isComplete.get
      valueOrNone <- request.get
      value <- valueOrNone.fold[F[Request]](F.raiseError(statusException(NoMessage)))(F.pure)
    } yield value
}

object Fs2UnaryServerCallListener {

  val TooManyRequests: String = "Too many requests"
  val NoMessage: String = "No message for unary call"

  private val statusException: String => StatusRuntimeException = msg =>
    new StatusRuntimeException(Status.INTERNAL.withDescription(msg))

  class PartialFs2UnaryServerCallListener[F[_]](val dummy: Boolean = false) extends AnyVal {

    def apply[Request, Response](
        call: ServerCall[Request, Response],
        signalReadiness: F[Unit],
        options: ServerCallOptions = ServerCallOptions.default
    )(implicit
        F: ConcurrentEffect[F]
    ): F[Fs2UnaryServerCallListener[F, Request, Response]] =
      for {
        request <- Ref.of[F, Option[Request]](none)
        isComplete <- Deferred[F, Unit]
        isCancelled <- Deferred[F, Unit]
        serverCall <- Fs2ServerCall[F, Request, Response](call, options)
      } yield new Fs2UnaryServerCallListener[F, Request, Response](request, signalReadiness, isComplete, isCancelled, serverCall)
  }

  def apply[F[_]] = new PartialFs2UnaryServerCallListener[F]
}
