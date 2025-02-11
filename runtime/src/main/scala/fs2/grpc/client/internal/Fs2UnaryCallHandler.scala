/*
 * Copyright (c) 2018 Gary Coady / Fs2 Grpc Developers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2.grpc.client.internal

import cats.effect.Sync
import cats.effect.SyncIO
import cats.effect.syntax.all._
import cats.effect.kernel.Async
import cats.effect.kernel.Outcome
import cats.effect.kernel.Ref
import cats.syntax.functor._
import cats.syntax.flatMap._
import fs2._
import fs2.grpc.client.ClientOptions
import io.grpc._

private[client] object Fs2UnaryCallHandler {
  sealed trait ReceiveState[R]

  object ReceiveState {
    def init[F[_]: Sync, R](
        callback: Either[Throwable, R] => Unit,
        pf: PartialFunction[StatusRuntimeException, Exception]
    ): F[Ref[SyncIO, ReceiveState[R]]] =
      Ref.in(new PendingMessage[R]({
        case r: Right[Throwable, R] => callback(r)
        case Left(e: StatusRuntimeException) => callback(Left(pf.lift(e).getOrElse(e)))
        case l: Left[Throwable, R] => callback(l)
      }))
  }

  class PendingMessage[R](callback: Either[Throwable, R] => Unit) extends ReceiveState[R] {
    def receive(message: R): PendingHalfClose[R] = new PendingHalfClose(callback, message)

    def sendError(error: Throwable): SyncIO[ReceiveState[R]] =
      SyncIO(callback(Left(error))).as(new Done[R])
  }

  class PendingHalfClose[R](callback: Either[Throwable, R] => Unit, message: R) extends ReceiveState[R] {
    def sendError(error: Throwable): SyncIO[ReceiveState[R]] =
      SyncIO(callback(Left(error))).as(new Done[R])

    def done: SyncIO[ReceiveState[R]] = SyncIO(callback(Right(message))).as(new Done[R])
  }

  class Done[R] extends ReceiveState[R]

  private def mkListener[Response](
      state: Ref[SyncIO, ReceiveState[Response]]
  ): ClientCall.Listener[Response] =
    new ClientCall.Listener[Response] {
      override def onMessage(message: Response): Unit =
        state.get
          .flatMap {
            case expected: PendingMessage[Response] =>
              state.set(expected.receive(message))
            case current: PendingHalfClose[Response] =>
              current
                .sendError(
                  Status.INTERNAL
                    .withDescription("More than one value received for unary call")
                    .asRuntimeException()
                )
                .flatMap(state.set)
            case _ => SyncIO.unit
          }
          .unsafeRunSync()

      override def onClose(status: Status, trailers: Metadata): Unit = {
        if (status.isOk) {
          state.get.flatMap {
            case expected: PendingHalfClose[Response] =>
              expected.done.flatMap(state.set)
            case current: PendingMessage[Response] =>
              current
                .sendError(
                  Status.INTERNAL
                    .withDescription("No value received for unary call")
                    .asRuntimeException(trailers)
                )
                .flatMap(state.set)
            case _ => SyncIO.unit
          }
        } else {
          state.get.flatMap {
            case current: PendingHalfClose[Response] =>
              current.sendError(status.asRuntimeException(trailers)).flatMap(state.set)
            case current: PendingMessage[Response] =>
              current.sendError(status.asRuntimeException(trailers)).flatMap(state.set)
            case _ => SyncIO.unit
          }
        }
      }.unsafeRunSync()
    }

  def unary[F[_], Request, Response](
      call: ClientCall[Request, Response],
      options: ClientOptions,
      message: Request,
      headers: Metadata
  )(implicit F: Async[F]): F[Response] = F.async[Response] { cb =>
    ReceiveState.init(cb, options.errorAdapter).map { state =>
      call.start(mkListener[Response](state), headers)
      // Initially ask for two responses from flow-control so that if a misbehaving server
      // sends more than one responses, we can catch it and fail it in the listener.
      call.request(2)
      call.sendMessage(message)
      call.halfClose()
      Some(onCancel(call))
    }
  }

  def stream[F[_], Request, Response](
      call: ClientCall[Request, Response],
      options: ClientOptions,
      messages: Stream[F, Request],
      headers: Metadata
  )(implicit F: Async[F]): F[Response] = F.async[Response] { cb =>
    ReceiveState.init(cb, options.errorAdapter).flatMap { state =>
      call.start(mkListener[Response](state), headers)
      // Initially ask for two responses from flow-control so that if a misbehaving server
      // sends more than one responses, we can catch it and fail it in the listener.
      call.request(2)
      messages
        .map(call.sendMessage)
        .compile
        .drain
        .guaranteeCase {
          case Outcome.Succeeded(_) => F.delay(call.halfClose())
          case Outcome.Errored(e) => F.delay(call.cancel(e.getMessage, e))
          case Outcome.Canceled() => onCancel(call)
        }
        .start
        .map(sending => Some(sending.cancel >> onCancel(call)))
    }
  }

  private def onCancel[F[_]](call: ClientCall[_, _])(implicit F: Async[F]): F[Unit] =
    F.delay(call.cancel("call was cancelled", null))

}
