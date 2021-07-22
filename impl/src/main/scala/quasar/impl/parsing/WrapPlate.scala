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

import cats.effect.Sync

import tectonic.{Plate, Signal}
import java.lang.CharSequence

private final class WrapPlate[A] private (
    keyName: String,
    delegate: Plate[A])
    extends Plate[A] {

  private[this] var seenSomething = false

  def nul(): Signal = {
    nestFurther()
    delegate.nul()
  }

  def fls(): Signal = {
    nestFurther()
    delegate.fls()
  }

  def tru(): Signal = {
    nestFurther()
    delegate.tru()
  }

  def map(): Signal = {
    nestFurther()
    delegate.map()
  }

  def arr(): Signal = {
    nestFurther()
    delegate.arr()
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    nestFurther()
    delegate.num(s, decIdx, expIdx)
  }

  def str(s: CharSequence): Signal = {
    nestFurther()
    delegate.str(s)
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    nestFurther()
    delegate.nestMap(pathComponent)
  }

  def nestArr(): Signal = {
    nestFurther()
    delegate.nestArr()
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    nestFurther()
    delegate.nestMeta(pathComponent)
  }

  def unnest(): Signal = {
    delegate.unnest()
  }

  def finishRow(): Unit = {
    unnestFurther()
    delegate.finishRow()
  }

  def finishBatch(terminal: Boolean): A = {
    delegate.finishBatch(terminal)
  }

  def skipped(bytes: Int): Unit = {
    delegate.skipped(bytes)
  }

  ////

  private[this] def nestFurther(): Unit = {
    if (!seenSomething) {
      seenSomething = true
      delegate.nestMap(keyName)
    }
  }

  private[this] def unnestFurther(): Unit = {
    if (seenSomething) {
      seenSomething = false
      delegate.unnest()
    }
  }
}

object WrapPlate {

  def apply[F[_]: Sync, A](keyName: String, delegate: Plate[A]): F[Plate[A]] =
    Sync[F].delay(new WrapPlate(keyName, delegate))
}
