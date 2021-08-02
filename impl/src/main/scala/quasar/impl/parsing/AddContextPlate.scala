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

import java.lang.CharSequence

import cats.effect.{IO, Sync}

import tectonic.{Plate, Signal}

final class AddContextPlate[A, B] private (contextKeyName: String, outputKeyName: String, delegate: Plate[A], emitter: Plate[A] => ValueEmitter[B]) extends Plate[A] {

  private val valueEmitter: ValueEmitter[B] = emitter(delegate)
  private val wrapDelegate = WrapPlate[IO, A](outputKeyName, delegate).unsafeRunSync()

  private[this] var sawNewRow = false
  private[this] var value: Option[B] = None
  private[this] var currentDelegate: Plate[A] = delegate

  def setValue(v: Option[B]): Unit =
    this.value = v

  def nul(): Signal = {
    emitOnNewRow()
    currentDelegate.nul()
  }

  def fls(): Signal = {
    emitOnNewRow()
    currentDelegate.fls()
  }

  def tru(): Signal = {
    emitOnNewRow()
    currentDelegate.tru()
  }

  def map(): Signal = {
    emitOnNewRow()
    currentDelegate.map()
  }

  def arr(): Signal = {
    emitOnNewRow()
    currentDelegate.arr()
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    emitOnNewRow()
    currentDelegate.num(s, decIdx, expIdx)
  }

  def str(s: CharSequence): Signal = {
    emitOnNewRow()
    currentDelegate.str(s)
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    emitOnNewRow()
    currentDelegate.nestMap(pathComponent)
  }

  def nestArr(): Signal = {
    emitOnNewRow()
    currentDelegate.nestArr()
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    emitOnNewRow()
    currentDelegate.nestMeta(pathComponent)
  }

  def unnest(): Signal = {
    emitOnNewRow()
    currentDelegate.unnest()
  }

  def finishRow(): Unit = {
    sawNewRow = false
    currentDelegate.finishRow()
  }

  def finishBatch(terminal: Boolean): A =
    currentDelegate.finishBatch(terminal)

  def skipped(bytes: Int): Unit =
    currentDelegate.skipped(bytes)

  private[this] def emitOnNewRow(): Unit = {
    if (!sawNewRow) {
      sawNewRow = true
      value match {
        case None =>
          currentDelegate = delegate

        case Some(v) =>
          currentDelegate = wrapDelegate

          delegate.nestMap(contextKeyName)
          valueEmitter.emit(v)
          delegate.unnest()
          ()
      }
    }
  }
}

object AddContextPlate {

  val PrecogContextKey = "precog_context_0c67cf80-b43f-45dd-9a8e-6382dbcff935"
  val PrecogOutputKey = "precog_output_bca7ccb9-87d6-443b-969f-6f287314b26d"

  def apply[F[_]: Sync, A, B](contextKeyName: String, outputKeyName: String, delegate: Plate[A], emitter: Plate[A] => ValueEmitter[B]): F[AddContextPlate[A, B]] =
    Sync[F].delay(new AddContextPlate(contextKeyName, outputKeyName, delegate, emitter))

  def mk[F[_]: Sync, A, B](delegate: Plate[A], emitter: Plate[A] => ValueEmitter[B]): F[AddContextPlate[A, B]] =
    apply(PrecogContextKey, PrecogOutputKey, delegate, emitter)
}
