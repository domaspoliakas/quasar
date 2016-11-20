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

package ygg

import common._
import scalaz._, Scalaz._, Ordering._

package object table {
  type TransSpec1   = trans.TransSpec1
  type TransSpec[A] = trans.TransSpec[A]
  type SourceType   = trans.SourceType

  type NeedSlices     = StreamT[Need, Slice]
  type RowId          = Int
  type Identity       = Long
  type Identities     = Array[Identity]
  type ColumnKV       = ColumnRef -> Column
  type ArrayColumnMap = Map[ColumnRef, ArrayColumn[_]]
  type ProjMap        = Map[Path, Projection]

  implicit def s2PathNode(name: String): CPathField = CPathField(name)
  implicit def i2PathNode(index: Int): CPathIndex   = CPathIndex(index)

  implicit def tableMethods[T: TableRep](table: T): TableMethods[T] = new TableMethods[T](table)

  private object compilerHelpers extends quasar.sql.CompilerHelpers {}
  import quasar.sql.{ Sql, Query, fixParser }

  def compileSql(q: String): Fix[Sql] = (fixParser parse Query(q)).toOption.get
  def compileLp(q: String): Fix[LP]   = compilerHelpers fullCompileExp q

  def evalSql[T: TableRep](table: T, q: String): T                            = EvalSql[T](table) eval compileSql(q)
  def evalLp[A: TableRep](files: Map[FPath, A], args: Sym => A, q: String): A = EvalLp[A](files, args) eval compileLp(q)

  def companionOf[T: TableRep] : TableCompanion[T] = TableRep[T].companion

  def lazyTable[T: TableRep](slices: Iterable[Slice], size: TableSize): T =
    lazyTable[T](StreamT fromStream Need(slices.toStream), size)

  def lazyTable[T: TableRep](slices: StreamT[Need, Slice], size: TableSize): T =
    companionOf[T].fromSlices(slices, size)

  def unfoldStream[A](start: A)(f: A => Need[Option[Slice -> A]]): StreamT[Need, Slice] = StreamT.unfoldM[Need, Slice, A](start)(f)
  def columnMap(xs: ColumnKV*): ColumnMap.Eager                                         = ColumnMap.Eager(xs.toVector)
  def lazyColumnMap(expr: => Seq[ColumnKV]): ColumnMap.Lazy                             = ColumnMap.Lazy(() => expr.toVector)

  def composeSliceTransform(spec: TransSpec1): SliceTransform1[_]             = SliceTransform.composeSliceTransform(spec)
  def composeSliceTransform2(spec: TransSpec[SourceType]): SliceTransform2[_] = SliceTransform.composeSliceTransform2(spec)

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): Cmp = {
    0 until prefixLength foreach { i =>
      ids1(i) ?|? ids2(i) match {
        case EQ  => ()
        case cmp => return cmp
      }
    }
    EQ
  }

  def prefixIdentityOrder(prefixLength: Int): Ord[Identities] =
    Ord order (prefixIdentityOrdering(_, _, prefixLength))

  def identityValueOrder[A: Ord](idOrder: Ord[Identities]): Ord[Identities -> A] =
    Ord order ((x, y) => idOrder.order(x._1, y._1) |+| (x._2 ?|? y._2))

  def fullIdentityOrdering(ids1: Identities, ids2: Identities): Cmp =
    prefixIdentityOrder(ids1.length min ids2.length)(ids1, ids2)

  def tupledIdentitiesOrder[A](ord: Ord[Identities]): Ord[Identities -> A] = ord contramap (_._1)
  def valueOrder[A](ord: Ord[A]): Ord[Identities -> A]                     = ord contramap (_._2)

  implicit def liftCF2(f: CF2) = new CF2Like {
    def applyl(cv: CValue) = CF1("builtin::liftF2::applyl")(f(Column const cv, _))
    def applyr(cv: CValue) = CF1("builtin::liftF2::applyr")(f(_, Column const cv))
    def andThen(f1: CF1)   = CF2("builtin::liftF2::andThen")((c1, c2) => f(c1, c2) flatMap f1.apply)
  }
}
