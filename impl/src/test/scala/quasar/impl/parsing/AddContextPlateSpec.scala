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

import cats.effect.IO

import org.specs2.mutable.Specification

import tectonic.test.{Event, ReifiedTerminalPlate}

object AddContextPlateSpec extends Specification {

  def mkPlate() = {
    val reified = ReifiedTerminalPlate[IO](true).unsafeRunSync()
    AddContextPlate[IO, List[Event], Map[String, CValue]]("ctx", "out", reified, new CValueMapEmitter(_)).unsafeRunSync()
  }

  "AddContextPlate" should {

    "when context set initially" in {

      "does not add anything when no output" in {
        val plate = mkPlate()
        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.finishBatch(true) mustEqual List.empty
      }

      "adds context and wraps output when output" in {
        val plate = mkPlate()
        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.str("some output")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("some output"),
          Event.Unnest,
          Event.FinishRow)
      }

      "adds context and wraps output for every row" in {
        val plate = mkPlate()
        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.str("output row 1")
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "picks up changes in context" in {
        val plate = mkPlate()
        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.str("output row 1")
        plate.finishRow()

        plate.setValue(Some(Map("id" -> CString("bye"))))

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("bye"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "changing context does not affect already started row" in {
        val plate = mkPlate()
        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.nestMap("nested")

        plate.setValue(Some(Map("id" -> CString("bye"))))

        plate.str("output row 1")
        plate.unnest()
        plate.finishRow()

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.NestMap("nested"),
          Event.Str("output row 1"),
          Event.Unnest,
          Event.Unnest,
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("bye"),
          Event.Unnest,
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

        plate.setValue(Some(Map("id" -> CString("hi"))))

        plate.str("output row 2")
        plate.finishRow()

        plate.finishBatch(true) mustEqual List(
          Event.Str("output row 1"),
          Event.FinishRow,

          Event.NestMap("ctx"),
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }

      "changing context does not affect already started row" in {
        val plate = mkPlate()

        plate.nestMap("nested")

        plate.setValue(Some(Map("id" -> CString("hi"))))

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
          Event.NestMap("id"),
          Event.Str("hi"),
          Event.Unnest,
          Event.Unnest,
          Event.NestMap("out"),
          Event.Str("output row 2"),
          Event.Unnest,
          Event.FinishRow)
      }
    }
  }
}
