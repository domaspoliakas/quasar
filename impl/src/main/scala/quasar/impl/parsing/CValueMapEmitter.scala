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

import tectonic.Plate

class CValueMapEmitter[A](plate: Plate[A]) extends ValueEmitter[Map[String, CValue]] {

  def emit(m: Map[String, CValue]): Unit =
    m.foreach { case (fld, value) =>
      plate.nestMap(fld)
      emitCValue(value)
      plate.unnest()
    }

  private def emitCValue(cv: CValue): Unit = {
    cv match {
      case CNull => plate.nul()
      case CBoolean(b) => if (b) plate.tru() else plate.fls()
      case CString(s) => plate.str(s)
      case CLong(l) => plate.num(l.toString, -1, -1)
      case CDouble(d) => emitNum(d.toString)
      case CNum(n) => emitNum(n.toString)
      case CEmptyObject => plate.map()
      case CEmptyArray => plate.arr()

      case _ => std.errorNotImplemented
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
    ()
  }

  private def emitNum(s: String) = {
    val iExp = {
      val ie = s.indexOf('e')
      if (ie == -1) s.indexOf('E') else ie
    }
    plate.num(s, s.indexOf('.'), iExp)
  }
}
