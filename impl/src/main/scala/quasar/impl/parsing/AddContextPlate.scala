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

import quasar.common.data.RValue

import java.lang.CharSequence

import cats.effect.{IO, Sync}

import tectonic.{Plate, Signal}

final class AddContextPlate[A] private (contextKeyName: String, outputKeyName: String, delegate: Plate[A]) extends Plate[A] {

  private val rValueEmitter = new RValueEmitter[A](delegate)
  private val wrapDelegate = WrapPlate[IO, A](outputKeyName, delegate).unsafeRunSync()

  private[this] var sawNewRow = false
  private[this] var rValue: Option[RValue] = None
  private[this] var currentDelegate: Plate[A] = delegate

  def setRValue(rv: Option[RValue]): Unit =
    this.rValue = rv

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
      rValue match {
        case None =>
          currentDelegate = delegate

        case Some(rv) =>
          currentDelegate = wrapDelegate

          delegate.nestMap(contextKeyName)
          rValueEmitter.emit(rv)
          delegate.unnest()
      }
    }
  }
}

object AddContextPlate {
  def apply[F[_]: Sync, A](contextKeyName: String, outputKeyName: String, delegate: Plate[A]): F[AddContextPlate[A]] =
    Sync[F].delay(new AddContextPlate(contextKeyName, outputKeyName, delegate))
}
