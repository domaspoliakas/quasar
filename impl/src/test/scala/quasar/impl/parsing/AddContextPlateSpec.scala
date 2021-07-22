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

package quasar.impl.parsing

import slamdata.Predef._

import quasar.common.data._

import scala.annotation.tailrec

import cats.effect.IO

import org.specs2.mutable.Specification

import tectonic.test.{Event, ReifiedTerminalPlate}

object AddContextPlateSpec extends Specification {

  def mkPlate() = {
    val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()
    AddContextPlate[IO, List[Event], RValue]("ctx", "out", reified, new RValueEmitter(_)).unsafeRunSync()
  }

  "AddContextPlate" should {

    "when context set initially" in {

      "does not add anything when no output" in {
        val plate = mkPlate()
        plate.setValue(Some(CString("some context")))

        plate.finishBatch(true) mustEqual List.empty
      }

      "adds context and wraps output when output" in {
        val plate = mkPlate()
        plate.setValue(Some(CString("some context")))

        plate.str("some output")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.Str("some context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("some output"),
          Event.Unnest,
          Event.FinishRow)
      }

      "adds context and wraps output for every row" in {
        val plate = mkPlate()
        plate.setValue(Some(CString("some context")))

        plate.str("output row 1")
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.Str("some context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.Str("some context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "picks up changes in context" in {
        val plate = mkPlate()
        plate.setValue(Some(CString("some context")))

        plate.str("output row 1")
        plate.finishRow()

        plate.setValue(Some(CString("other context")))

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.Str("some context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.Str("other context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "changing context does not affect already started row" in {
        val plate = mkPlate()
        plate.setValue(Some(CString("some context")))

        plate.nestMap("nested")

        plate.setValue(Some(CString("other context")))

        plate.str("output row 1")
        plate.unnest()
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.Str("some context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.NestMap("nested"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.Str("other context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }
    }

    "when no context set initially" in {

      "does not add anything when no output" in {
        val plate = mkPlate()

        plate.finishBatch(true) mustEqual List.empty
      }

      "does not wrap output when output" in {
        val plate = mkPlate()

        plate.str("some output")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.Str("some output"),
          Event.FinishRow)
      }

      "does not wrap output for any row" in {
        val plate = mkPlate()

        plate.str("output row 1")
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.Str("output row 1"),
          Event.FinishRow,

          Event.Str("output row 2"),
          Event.FinishRow)
      }

      "picks up changes in context" in {
        val plate = mkPlate()

        plate.str("output row 1")
        plate.finishRow()

        plate.setValue(Some(CString("other context")))

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.Str("output row 1"),
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.Str("other context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "changing context does not affect already started row" in {
        val plate = mkPlate()

        plate.nestMap("nested")

        plate.setValue(Some(CString("other context")))

        plate.str("output row 1")
        plate.unnest()
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("nested"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.Str("other context"),
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }
    }
  }

  "deep structures" in {

    @tailrec
    def mkObj(rv: RValue, i: Int): RValue =
      if (i <= 0) rv
      else mkObj(RObject(Map("a" -> rv)), i - 1)

    "deep object should not stack overflow" in {
      val plate = mkPlate()
      val nesting = 10000
      plate.setValue(Some(mkObj(CString("q"), nesting)))

      plate.str("some output")
      plate.finishRow()

      val nests = List.fill(nesting)(Event.NestMap("a"))
      val unnests = List.fill(nesting + 1)(Event.Unnest)

      val expected =
        List(Event.NestMap("ctx")) ++
          nests ++
          List(Event.Str("q")) ++
          unnests ++
          List(
            Event.NestMap("out"),
            Event.Str("some output"),
            Event.Unnest,
            Event.FinishRow)

      //plate.finishBatch(true) mustEqual expected
      plate.finishBatch(true).size mustEqual expected.size
    }

    @tailrec
    def mkArr(rv: RValue, i: Int): RValue =
      if (i <= 0) rv
      else mkArr(RArray(rv), i - 1)

    "deep array should not stack overflow" in {
      val plate = mkPlate()
      val nesting = 10000
      plate.setValue(Some(mkArr(CString("q"), nesting)))

      plate.str("some output")
      plate.finishRow()

      val nests = List.fill(nesting)(Event.NestArr)
      val unnests = List.fill(nesting + 1)(Event.Unnest)

      val expected =
        List(Event.NestMap("ctx")) ++
          nests ++
          List(Event.Str("q")) ++
          unnests ++
          List(
            Event.NestMap("out"),
            Event.Str("some output"),
            Event.Unnest,
            Event.FinishRow)

      // plate.finishBatch(true) mustEqual expected
      plate.finishBatch(true).size mustEqual expected.size
    }
  }
}
