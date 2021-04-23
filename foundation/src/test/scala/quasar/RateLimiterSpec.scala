/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import slamdata.Predef._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.UUID

import org.specs2.mutable.Specification

import fs2.Stream

import cats.effect._
import cats.effect.laws.util.TestContext
import cats.kernel.Hash
import cats.implicits._

object RateLimiterSpec extends Specification {

  import cats.effect.IO._

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)
  implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)

  def freshKey: IO[UUID] = IO.delay(UUID.randomUUID())

  "rate limiter" should {
    "output events with real time" >> {
      "one event in one window" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back = Stream.eval_(limit) ++ Stream.emit(1)

        back.compile.toList.unsafeRunSync() mustEqual(List(1))
      }

      "two events in one window" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val back =
          Stream.eval_(limit) ++ Stream.emit(1) ++
            Stream.eval_(limit) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "two events in two windows" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back =
          Stream.eval_(limit) ++ Stream.emit(1) ++
            Stream.eval_(limit) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "events from two keys" in {
        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        val back1 =
          Stream.eval_(limit1) ++ Stream.emit(1) ++
            Stream.eval_(limit1) ++ Stream.emit(2)

        val back2 =
          Stream.eval_(limit2) ++ Stream.emit(3) ++
            Stream.eval_(limit2) ++ Stream.emit(4)

        back1.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
        back2.compile.toList.unsafeRunSync() mustEqual(List(3, 4))
      }
    }

    "output events with simulated time" >> {
      "one event per second" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "one event per two seconds" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 1, 2.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "two events per second" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "three events per second" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit, _) =
          key.flatMap(k => rl(k, 3, 1.seconds)).unsafeRunSync()

        var a: Int = 0

        val run =
          limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1) >>
            limit >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "do not overwrite configs (use existing config)" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val k1 = key.unsafeRunSync()

        val RateLimiterEffects(limit1, _) = rl(k1, 2, 1.seconds).unsafeRunSync()
        val RateLimiterEffects(limit2, _) = rl(k1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1) >>
            limit2 >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "support two keys on the same schedule" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 3, 1.seconds)).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1)

        val run2 =
          limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(3)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(6)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(8)
      }

      "support two keys on different schedules" in {
        val ctx = TestContext()
        val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

        val RateLimiting(rl, key) =
          RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

        val RateLimiterEffects(limit1, _) =
          key.flatMap(k => rl(k, 2, 1.seconds)).unsafeRunSync()

        val RateLimiterEffects(limit2, _) =
          key.flatMap(k => rl(k, 2, 2.seconds)).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1) >>
            limit1 >> IO.delay(a1 += 1)

        val run2 =
          limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1) >>
            limit2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(6)
      }
    }

    "foobar respect backoff effect" in {
      val ctx = TestContext()
      val concurrent = IO.ioConcurrentEffect(ctx.contextShift[IO])

      val RateLimiting(rl, key) =
        RateLimiter[IO, UUID](freshKey)(concurrent, ctx.timer[IO], Hash[UUID]).unsafeRunSync()

      val RateLimiterEffects(limit, backoff) =
        key.flatMap(k => rl(k, 1, 2.seconds)).unsafeRunSync()

      var a: Int = 0

      val run =
        backoff >>
        limit >> IO.delay(a += 1) >>
        backoff >>
        limit >> IO.delay(a += 1) >>
        backoff >>
        limit >> IO.delay(a += 1)

      run.unsafeRunAsyncAndForget()

      a mustEqual(0)

      ctx.tick(1.seconds)
      a mustEqual(0)

      ctx.tick(1.seconds)
      a mustEqual(1)

      ctx.tick(1.seconds)
      a mustEqual(1)

      ctx.tick(1.seconds)
      a mustEqual(2)

      ctx.tick(1.seconds)
      a mustEqual(2)

      ctx.tick(1.seconds)
      a mustEqual(3)
    }
  }
}
