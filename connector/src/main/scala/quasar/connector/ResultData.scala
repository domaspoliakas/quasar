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

package quasar.connector

import slamdata.Predef._
import quasar.api.push
import quasar.common.data.CValue

import cats._
import cats.implicits._
import fs2.{Stream, Chunk}

sealed trait ResultData[F[_], A] extends Product with Serializable { self =>
  type Elem = A
  def delimited: Stream[F, ResultData.Part[A]]
  def data: Stream[F, A]
  def mapK[G[_]](f: F ~> G): ResultData[G, A]
}

object ResultData {

  sealed trait Part[+A] extends Product with Serializable {
    def fold[B](
        f1: push.ExternalOffsetKey => B,
        f2: Option[Map[String, CValue]] => B,
        f3: Chunk[A] => B)
        : B =
      this match {
        case Part.ExternalOffsetKey(k) => f1(k)
        case Part.ExternalIdentities(e) => f2(e)
        case Part.Output(c) => f3(c)
      }
  }

  sealed trait DataPart[+A] extends Part[A] {
    def foldData[B](
        f1: Option[Map[String, CValue]] => B,
        f2: Chunk[A] => B)
        : B =
      this match {
        case Part.ExternalIdentities(e) => f1(e)
        case Part.Output(c) => f2(c)
      }
  }

  object Part {
    final case class ExternalOffsetKey(offsetKey: push.ExternalOffsetKey) extends Part[Nothing]

    val emptyOffsetKey: ExternalOffsetKey = ExternalOffsetKey(push.ExternalOffsetKey.empty)

    def offsetKeyFromBytes(bytes: Array[Byte]): ExternalOffsetKey =
      ExternalOffsetKey(push.ExternalOffsetKey(bytes))

    /**
      * External identities pertinent to the output that follows, until another external identity annotation is
      * encountered e.g. Stream(E1, O1, O2, E2, O3, E3, E4, O4).
      *
      * E1 pertains to O1 and O2
      * E2 pertains to O3
      * E3 pertains to nothing
      * E4 pertains to O4
      *
      * @param value The external identities to set. A value of `None` means that there are no ids and the output that follows
      * will not be wrapped. In case of a context of `Some` the output that follows will be wrapped, even if the `Map`
      * of fields is empty.
      */
    final case class ExternalIdentities(value: Option[Map[String, CValue]]) extends DataPart[Nothing]

    final case class Output[A](chunk: Chunk[A]) extends DataPart[A]

    implicit val functorPart: Functor[Part] = new Functor[Part] {
      def map[A, B](fa: Part[A])(f: A => B): Part[B] = fa match {
        case Output(c) => Output(c.map(f))
        case e @ ExternalOffsetKey(_) => e
        case e @ ExternalIdentities(_) => e
      }
    }

    implicit val functorDataPart: Functor[DataPart] = new Functor[DataPart] {
      def map[A, B](fa: DataPart[A])(f: A => B): DataPart[B] = fa match {
        case Output(c) => Output(c.map(f))
        case e @ ExternalIdentities(_) => e
      }
    }
  }

  final case class Delimited[F[_], A](
      delimited: Stream[F, Part[A]])
      extends ResultData[F, A] {
    def data: Stream[F, A] = delimited.flatMap(_.fold(_ => Stream.empty, _ => Stream.empty, Stream.chunk))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Delimited(delimited.translate[F, G](f))
  }

  final case class Continuous[F[_], A](data: Stream[F, A]) extends ResultData[F, A] {
    def delimited = data.chunks.map(Part.Output(_))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Continuous(data.translate[F, G](f))
  }

  implicit def functorResultData[F[_]]: Functor[ResultData[F, *]] = new Functor[ResultData[F, *]] {
    def map[A, B](fa: ResultData[F,A])(f: A => B): ResultData[F,B] = fa match {
      case Continuous(data) => Continuous(data.map(f))
      case Delimited(delimited) => Delimited(delimited.map(_.map(f)))
    }
  }

  def empty[F[_], A]: ResultData[F, A] = Continuous(Stream.empty)
}
