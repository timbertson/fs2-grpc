package org.lyranthe.fs2_grpc
package java_runtime
package server

import cats.effect.{ContextShift, IO}
import cats.effect.laws.util.TestContext
import cats.implicits._
import fs2._
import io.grpc._
import minitest._
import org.lyranthe.fs2_grpc.java_runtime.shared.Readiness

object ServerSuite extends SimpleTestSuite {

  private[this] val compressionOps = ServerCallOptions.default.withServerCompressor(Some(GzipCompressor))

  test("single message to unaryToUnary")(singleUnaryToUnary())

  test("single message to unaryToUnary with compression")(singleUnaryToUnary(compressionOps))

  private[this] def singleUnaryToUnary(options: ServerCallOptions = ServerCallOptions.default): Unit = {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2UnaryServerCallListener[IO](dummy, options).unsafeRunSync()

    listener.unsafeUnaryResponse(new Metadata(), _.map(_.length))
    listener.onMessage("123")
    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.messages.size, 1)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  test("cancellation for unaryToUnary") {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2UnaryServerCallListener[IO](dummy).unsafeRunSync()

    listener.unsafeUnaryResponse(new Metadata(), _.map(_.length))
    listener.onCancel()

    val cancelled = listener.isCancelled.get.unsafeToFuture()

    ec.tick()

    assertEquals(cancelled.isCompleted, true)
  }

  test("multiple messages to unaryToUnary")(multipleUnaryToUnary())

  test("multiple messages to unaryToUnary with compression")(multipleUnaryToUnary(compressionOps))

  private[this] def multipleUnaryToUnary(options: ServerCallOptions = ServerCallOptions.default): Unit = {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2UnaryServerCallListener[IO](dummy, options).unsafeRunSync()

    listener.unsafeUnaryResponse(new Metadata(), _.map(_.length))
    listener.onMessage("123")

    intercept[StatusRuntimeException] {
      listener.onMessage("456")
    }

    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.currentStatus.isDefined, true)
    assertResult(true, "Current status true because stream completed successfully")(dummy.currentStatus.get.isOk)
  }

  test("resource awaits termination of server") {

    implicit val ec: TestContext = TestContext()
    import implicits._

    val result = ServerBuilder.forPort(0).resource[IO].use(IO.pure).unsafeToFuture()
    ec.tick()
    val server = result.value.get.get
    assert(server.isTerminated)
  }

  test("single message to unaryToStreaming")(singleUnaryToStreaming())

  test("single message to unaryToStreaming witn compression")(singleUnaryToStreaming(compressionOps))

  private[this] def singleUnaryToStreaming(options: ServerCallOptions = ServerCallOptions.default): Unit = {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2UnaryServerCallListener[IO].apply[String, Int](dummy, options).unsafeRunSync()

    listener.unsafeStreamResponse(Readiness.noop, new Metadata(), s => Stream.eval(s).map(_.length).repeat.take(5))
    listener.onMessage("123")
    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.messages.size, 5)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  test("zero messages to streamingToStreaming") {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2StreamServerCallListener[IO].apply[String, Int](dummy, IO.unit).unsafeRunSync()

    listener.unsafeStreamResponse(Readiness.noop, new Metadata(), _ => Stream.emit(3).repeat.take(5))
    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.messages.size, 5)
    assertEquals(dummy.messages(0), 3)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  test("cancellation for streamingToStreaming") {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2StreamServerCallListener[IO].apply[String, Int](dummy, IO.unit).unsafeRunSync()

    listener.unsafeStreamResponse(Readiness.noop, new Metadata(), _ => Stream.emit(3).repeat.take(5))

    listener.onCancel()

    val cancelled = listener.isCancelled.get.unsafeToFuture()

    ec.tick()

    assertEquals(cancelled.isCompleted, true)
  }

  test("messages to streamingToStreaming")(multipleStreamingToStreaming())

  test("messages to streamingToStreaming with compression")(multipleStreamingToStreaming(compressionOps))

  private[this] def multipleStreamingToStreaming(options: ServerCallOptions = ServerCallOptions.default): Unit = {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2StreamServerCallListener[IO].apply[String, Int](dummy, IO.unit, options).unsafeRunSync()

    listener.unsafeStreamResponse(Readiness.noop, new Metadata(), _.map(_.length).intersperse(0))
    listener.onMessage("a")
    listener.onMessage("ab")
    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.messages.length, 3)
    assertEquals(dummy.messages.toList, List(1, 0, 2))
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

  test("messages to streamingToStreaming") {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val dummy = new DummyServerCall
    val listener = Fs2StreamServerCallListener[IO].apply[String, Int](dummy, IO.unit).unsafeRunSync()

    listener.unsafeStreamResponse(
      Readiness.noop,
      new Metadata(),
      _.map(_.length) ++ Stream.emit(0) ++ Stream.raiseError[IO](new RuntimeException("hello"))
    )
    listener.onMessage("a")
    listener.onMessage("ab")
    listener.onHalfClose()
    listener.onMessage("abc")

    ec.tick()

    assertEquals(dummy.messages.length, 3)
    assertEquals(dummy.messages.toList, List(1, 2, 0))
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, false)
  }

  test("streaming to unary")(streamingToUnary())

  test("streaming to unary with compression")(streamingToUnary(compressionOps))

  private[this] def streamingToUnary(options: ServerCallOptions = ServerCallOptions.default): Unit = {

    implicit val ec: TestContext = TestContext()
    implicit val cs: ContextShift[IO] = IO.contextShift(ec)

    val implementation: Stream[IO, String] => IO[Int] =
      _.compile.foldMonoid.map(_.length)

    val dummy = new DummyServerCall
    val listener = Fs2StreamServerCallListener[IO].apply[String, Int](dummy, IO.unit, options).unsafeRunSync()

    listener.unsafeUnaryResponse(new Metadata(), implementation)
    listener.onMessage("ab")
    listener.onMessage("abc")
    listener.onHalfClose()

    ec.tick()

    assertEquals(dummy.messages.length, 1)
    assertEquals(dummy.messages(0), 5)
    assertEquals(dummy.currentStatus.isDefined, true)
    assertEquals(dummy.currentStatus.get.isOk, true)
  }

}
