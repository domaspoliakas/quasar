/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.Predef.List
import quasar.fp._
import quasar.qscript.MapFuncs._
import quasar.sql.CompilerHelpers

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptPruneArraysSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val PA = new PAFindReplace[Fix, QST]

  "prune arrays" should {
    "rewrite map-filter with unused array elements" in {
      def initial(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(BoolLit(true))))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      def expected(src: QST[Fix[QST]]): Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              src.embed,
              HoleF,
              ExcludeId,
              MakeArrayR(BoolLit(true)))).embed,
            ProjectIndexR(HoleF, IntLit(0)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial(UnreferencedRT)) must equal(expected(UnreferencedRT))
      PA.pruneArrays.apply(initial(RootRT)) must equal(expected(RootRT))

      val data = rootDir </> file("zips")
      PA.pruneArrays.apply(initial(ReadRT(data))) must equal(expected(ReadRT(data)))
    }

    "not rewrite map-filter with no unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(Filter(
            QCT.inj(LeftShift(
              UnreferencedRT.embed,
              HoleF,
              ExcludeId,
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(BoolLit(true))))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }

    "not rewrite filter with unused array elements" in {
      val initial: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(BoolLit(true))))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }

    "rewrite map with unused array elements 1,2" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(IntLit(8))))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(6)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "rewrite map with unused array elements 0,2" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(IntLit(8))))).embed,
          ProjectIndexR(HoleF, IntLit(1)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(7)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "rewrite map with unused array elements 0,1" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(IntLit(8))))).embed,
          ProjectIndexR(HoleF, IntLit(2)))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            MakeArrayR(IntLit(8)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "rewrite map with unused array elements in a binary map func" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(IntLit(8))))).embed,
          AddR(
            ProjectIndexR(HoleF, IntLit(2)),
            ProjectIndexR(HoleF, IntLit(0))))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(8))))).embed,
          AddR(
            ProjectIndexR(HoleF, IntLit(1)),
            ProjectIndexR(HoleF, IntLit(0))))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "not rewrite leftshift with nonstatic array dereference" in {
      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            UnreferencedRT.embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(IntLit(6)),
                MakeArrayR(IntLit(7))),
              MakeArrayR(IntLit(8))))).embed,
          AddR(
            ProjectIndexR(HoleF, IntLit(2)),
            ProjectIndexR(HoleF, AddR(IntLit(0), IntLit(1)))))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }

    "rewrite two leftshift arrays" in {
      val innerInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // [6, [7], 8]
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(MakeArrayR(IntLit(7)))),
            MakeArrayR(IntLit(8))))).embed

      val initial: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            QCT.inj(Map(
              innerInitial,
              ProjectIndexR(HoleF, IntLit(1)))).embed,
            HoleF,
            ExcludeId,
            ConcatArraysR(LeftSideF, RightSideF))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val innerExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          MakeArrayR(MakeArrayR(IntLit(7))))).embed

      val expected: Fix[QST] =
        QCT.inj(Map(
          QCT.inj(LeftShift(
            QCT.inj(Map(
              innerExpected,
              ProjectIndexR(HoleF, IntLit(0)))).embed,
            HoleF,
            ExcludeId,
            LeftSideF[Fix])).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "rewrite filter-map-filter-leftshift" in {
      val innerArray: JoinFunc =
        ConcatArraysR(     // [7, 8, 9]
          ConcatArraysR(
            MakeArrayR(IntLit(7)),
            MakeArrayR(IntLit(8))),
          MakeArrayR(IntLit(9)))

      val srcInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // ["a", [7, 8, 9], true]
            ConcatArraysR(
              MakeArrayR(StrLit("a")),
              MakeArrayR(innerArray)),
            MakeArrayR(BoolLit(true))))).embed

      val initial: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(Map(
            QCT.inj(Filter(
              srcInitial,
              ProjectIndexR(HoleF, IntLit(2)))).embed,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      val srcExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(      // [[7, 8, 9], true]
            MakeArrayR(innerArray),
            MakeArrayR(BoolLit(true))))).embed

      val expected: Fix[QST] =
        QCT.inj(Filter(
          QCT.inj(Map(
            QCT.inj(Filter(
              srcExpected,
              ProjectIndexR(HoleF, IntLit(1)))).embed,
            ProjectIndexR(HoleF, IntLit(0)))).embed,
          ProjectIndexR(HoleF, IntLit(0)))).embed

      PA.pruneArrays.apply(initial) must equal(expected)
    }

    "rewrite reduce-filter-leftshift" in {
      val srcInitial: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              ConcatArraysR(
                MakeArrayR(StrLit("a")),
                MakeArrayR(StrLit("b"))),
              MakeArrayR(StrLit("c"))),
            MakeArrayR(StrLit("d"))))).embed

      val initial: QST[Fix[QST]] =
        QCT.inj(Reduce(
          QCT.inj(Filter(
            srcInitial,
            ProjectIndexR(HoleF, IntLit(3)))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(HoleF, IntLit(2)))),
          MakeMapR(IntLit(0), ReduceIndexF(0))))

      val srcExpected: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            MakeArrayR(StrLit("c")),
            MakeArrayR(StrLit("d"))))).embed

      val expected: QST[Fix[QST]] =
        QCT.inj(Reduce(
          QCT.inj(Filter(
            srcExpected,
            ProjectIndexR(HoleF, IntLit(1)))).embed,
          NullLit(),
          List(ReduceFuncs.Count(ProjectIndexR(HoleF, IntLit(0)))),
          MakeMapR(IntLit(0), ReduceIndexF(0))))

      PA.pruneArrays.apply(initial.embed) must equal(expected.embed)
    }

    // this can be rewritten - we just don't support that yet
    "not rewrite theta join with unused array elements" in {
      val src: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(IntLit(8))))).embed

      val initial: Fix[QST] =
        TJT.inj(ThetaJoin(
          src,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          HoleQS,
          BoolLit[Fix, JoinSide](true),
          Inner,
          MakeMapR(StrLit("xyz"), Free.point(LeftSide)))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }

    // this can be rewritten - we just don't support that yet
    "not rewrite equi join with unused array elements" in {
      val src: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(IntLit(8))))).embed

      val initial: Fix[QST] =
        EJT.inj(EquiJoin(
          src,
          Free.roll(QCT.inj(Map(
            HoleQS,
            ProjectIndexR(HoleF, IntLit[Fix, Hole](2))))),
          HoleQS,
          HoleF,
          HoleF,
          Inner,
          MakeMapR(StrLit("xyz"), Free.point(LeftSide)))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }

    // this can be rewritten - we just don't support that yet
    "not rewrite left shift with unused array elements" in {
      val src: Fix[QST] =
        QCT.inj(LeftShift(
          UnreferencedRT.embed,
          HoleF,
          ExcludeId,
          ConcatArraysR(
            ConcatArraysR(
              MakeArrayR(IntLit(6)),
              MakeArrayR(IntLit(7))),
            MakeArrayR(IntLit(8))))).embed

      val initial: Fix[QST] =
        QCT.inj(LeftShift(
          src,
          ProjectIndexR(HoleF, IntLit[Fix, Hole](2)),
          ExcludeId,
          MakeMapR(StrLit("xyz"), Free.point(LeftSide)))).embed

      PA.pruneArrays.apply(initial) must equal(initial)
    }
  }
}
