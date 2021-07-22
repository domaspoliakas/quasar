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
import quasar.contrib.std

import scala.util.control.TailCalls._

import tectonic.Plate

class RValueEmitter[A](plate: Plate[A]) extends ValueEmitter[RValue] {

  def emit(rv: RValue): Unit = emitRec(rv).result

  private def emitRec(rv: RValue): TailRec[Unit] = {
    rv match {
      case CNull => done { plate.nul(); () }
      case CBoolean(b) => done { if (b) plate.tru() else plate.fls(); () }
      case CString(s) => done { plate.str(s); () }
      case CLong(l) => done { plate.num(l.toString, -1, -1); () }
      case CDouble(d) => done { emitNum(d.toString); () }
      case CNum(n) => done { emitNum(n.toString); () }
      case RObject(o) => emitEntries(o.toList)
      case RArray(a) => emitAll(a)
      case CEmptyObject => done { plate.map(); () }
      case CEmptyArray => done { plate.arr(); () }

      case _ => std.errorNotImplemented
      // case RMeta(_, _) => ()
      // case COffsetDateTime(_) => ()
      // case COffsetDate(_) => ()
      // case COffsetTime(_) => ()
      // case CLocalDateTime(_) => ()
      // case CLocalDate(_) => ()
      // case CLocalTime(_) => ()
      // case CInterval(_) => ()
      // case CBinary(_) => ()
      // case CUndefined => ()
      // case CArray(_, _) => ()
    }
  }

  private def emitEntries(l: List[(String, RValue)]): TailRec[Unit] =
    l match {
      case (f, v) :: tl =>
        for {
          _ <- done(plate.nestMap(f))
          _ <- tailcall(emitRec(v))
          _ <- done(plate.unnest())
          _ <- tailcall(emitEntries(tl))
        } yield (())
      case Nil => done(())
    }

  private def emitAll(l: List[RValue]): TailRec[Unit] =
    l match {
      case h :: tl =>
        for {
          _ <- done(plate.nestArr())
          _ <- tailcall(emitRec(h))
          _ <- done(plate.unnest())
          _ <- tailcall(emitAll(tl))
        } yield (())
      case Nil => done(())
    }

  private def emitNum(s: String) = {
    val iExp = {
      val ie = s.indexOf('e')
      if (ie == -1) s.indexOf('E') else ie
    }
    plate.num(s, s.indexOf('.'), iExp)
  }
}
