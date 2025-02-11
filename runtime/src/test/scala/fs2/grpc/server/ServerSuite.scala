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

package fs2
package grpc
package server

import scala.concurrent.duration._
import cats.effect._
import cats.effect.std.Dispatcher
import cats.effect.testkit.TestContext
import fs2.grpc.server.internal.Fs2UnaryServerCallHandler
import io.grpc._

class ServerSuite extends Fs2GrpcSuite {

  private val compressionOps =
    ServerOptions.default.configureCallOptions(_.withServerCompressor(Some(GzipCompressor)))

  runTest("single message to unaryToUnary")(singleUnaryToUnary())
  runTest("single message to unaryToUnary with compression")(singleUnaryToUnary(compressionOps))

  private[this] def singleUnaryToUnary(
      options: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2UnaryServerCallHandler.unary[IO, String, Int]((req, _) => IO(req.length), options, d)
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("123")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.size, 1)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  runTest("cancellation for unaryToUnary") { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2UnaryServerCallHandler.unary[IO, String, Int]((req, _) => IO(req.length), ServerOptions.default, d)
    val listener = handler.startCall(dummy, new Metadata())

    listener.onCancel()
    tc.tick()

    assertEquals(dummy.currentStatus, None)
    assertEquals(dummy.messages.length, 0)
  }

  runTest("cancellation on the fly for unaryToUnary") { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2UnaryServerCallHandler.unary[IO, String, Int](
      (req, _) => IO(req.length).delayBy(10.seconds),
      ServerOptions.default,
      d
    )
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("123")
    listener.onHalfClose()
    tc.tick()
    listener.onCancel()
    tc.tick()

    assertEquals(dummy.currentStatus.map(_.getCode), Some(Status.Code.CANCELLED))
    assertEquals(dummy.messages.length, 0)
  }

  runTest("multiple messages to unaryToUnary")(multipleUnaryToUnary())
  runTest("multiple messages to unaryToUnary with compression")(multipleUnaryToUnary(compressionOps))

  private def multipleUnaryToUnary(
      options: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2UnaryServerCallHandler.unary[IO, String, Int]((req, _) => IO(req.length), options, d)
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("123")
    listener.onMessage("456")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.currentStatus.map(_.getCode), Some(Status.Code.INTERNAL))
  }

  runTest("no messages to unaryToUnary")(noMessageUnaryToUnary())
  runTest("no messages to unaryToUnary with compression")(noMessageUnaryToUnary(compressionOps))

  private def noMessageUnaryToUnary(
      options: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2UnaryServerCallHandler.unary[IO, String, Int]((req, _) => IO(req.length), options, d)
    val listener = handler.startCall(dummy, new Metadata())

    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.currentStatus.map(_.getCode), Some(Status.Code.INTERNAL))
  }

  runTest0("resource awaits termination of server") { (tc, r, _) =>
    import fs2.grpc.syntax.all._
    import netty.shaded.io.grpc.netty.NettyServerBuilder

    val result = NettyServerBuilder.forPort(0).resource[IO].use(IO.pure).unsafeToFuture()(r)
    tc.tick()

    val server = result.value.get.get
    assert(server.isTerminated)
  }

  runTest("single message to unaryToStreaming")(singleUnaryToStreaming())
  runTest("single message to unaryToStreaming with compression")(singleUnaryToStreaming(compressionOps))

  private def singleUnaryToStreaming(
      options: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val dummy = new DummyServerCall
    val handler =
      Fs2UnaryServerCallHandler.stream[IO, String, Int]((s, _) => Stream(s).map(_.length).repeat.take(5), options, d)
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("123")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.size, 5)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  runTest("zero messages to streamingToStreaming") { (tc, d) =>
    val dummy = new DummyServerCall

    val handler = Fs2ServerCallHandler[IO](d, ServerOptions.default)
      .streamingToStreamingCall[String, Int]((_, _) => Stream.emit(3).repeat.take(5))
    val listener = handler.startCall(dummy, new Metadata())

    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.size, 5)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  runTest("cancellation for streamingToStreaming") { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2ServerCallHandler[IO](d, ServerOptions.default)
      .streamingToStreamingCall[String, Int]((_, _) =>
        Stream.emit(3).repeat.take(5).zipLeft(Stream.awakeDelay[IO](1.seconds))
      )
    val listener = handler.startCall(dummy, new Metadata())

    tc.tick()
    listener.onCancel()
    tc.tick()

    assertEquals(dummy.currentStatus.map(_.getCode), Some(Status.Code.CANCELLED))
  }

  runTest("messages to streamingToStreaming")(multipleStreamingToStreaming())
  runTest("messages to streamingToStreaming with compression")(multipleStreamingToStreaming(compressionOps))

  private def multipleStreamingToStreaming(
      options: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val dummy = new DummyServerCall
    val handler = Fs2ServerCallHandler[IO](d, options)
      .streamingToStreamingCall[String, Int]((req, _) => req.map(_.length).intersperse(0))
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("a")
    listener.onMessage("ab")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.length, 3)
    assertEquals(dummy.messages.toList, List(1, 0, 2))
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  runTest("messages to streamingToStreaming with error") { (tc, d) =>
    val dummy = new DummyServerCall
    val error = new RuntimeException("hello")

    val handler = Fs2ServerCallHandler[IO](d, ServerOptions.default)
      .streamingToStreamingCall[String, Int]((req, _) =>
        req.map(_.length) ++ Stream.emit(0) ++ Stream.raiseError[IO](error)
      )
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("a")
    listener.onMessage("ab")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.length, 3)
    assertEquals(dummy.messages.toList, List(1, 2, 0))
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, false)
  }

  runTest("streaming to unary")(streamingToUnary())
  runTest("streaming to unary with compression")(streamingToUnary(compressionOps))

  private def streamingToUnary(
      so: ServerOptions = ServerOptions.default
  ): (TestContext, Dispatcher[IO]) => Unit = { (tc, d) =>
    val implementation: Stream[IO, String] => IO[Int] =
      _.compile.foldMonoid.map(_.length)

    val dummy = new DummyServerCall

    val handler = Fs2ServerCallHandler[IO](d, so)
      .streamingToUnaryCall[String, Int]((req, _) => implementation(req))
    val listener = handler.startCall(dummy, new Metadata())

    listener.onMessage("ab")
    listener.onMessage("abc")
    listener.onHalfClose()
    tc.tick()

    assertEquals(dummy.messages.length, 1)
    assertEquals(dummy.messages(0), 5)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

}
