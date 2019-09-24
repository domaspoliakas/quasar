/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.storage

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.impl.cluster.Timestamped, Timestamped._

import cats.~>
import cats.effect.{Timer, Concurrent, Sync, Resource, ContextShift}
import cats.effect.concurrent.Ref
import cats.syntax.functor._
import cats.syntax.flatMap._

import fs2.Stream
import fs2.concurrent.Queue

import scalaz.syntax.tag._

final class TimestampedStore[F[_]: Sync: Timer, K, V](
    val underlying: IndexedStore[F, K, Timestamped[V]],
    ref: Ref[F, Map[K, Long]],
    queue: Queue[F, (K, Timestamped[V])])
    extends IndexedStore[F, K, V] {

  def entries: Stream[F, (K, V)] =
    underlying.entries.map({ case (k, v) => raw(v).map((k, _))}).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(raw(_)))

  def insert(k: K, v: V): F[Unit] = for {
    toInsert <- tagged[F, V](v)
    _ <- underlying.insert(k, toInsert)
    _ <- ref.modify((mp: Map[K, Long]) => (mp.updated(k, timestamp(toInsert)), ()))
    _ <- queue.enqueue1((k, toInsert))
  } yield ()

  def delete(k: K): F[Boolean] = for {
    was <- underlying.lookup(k)
    tmb <- tombstone[F, V]
    res <- underlying.insert(k, tmb)
    _ <- ref.modify((mp: Map[K, Long]) => (mp.updated(k, timestamp(tmb)), ()))
    _ <- queue.enqueue1((k, tmb))
  } yield was.flatMap(raw(_)).nonEmpty

  def timestamps: F[Map[K, Long]] = ref.get
  def updates: Stream[F, (K, Timestamped[V])] = queue.dequeue
}

object TimestampedStore {
  val QueueSize: Int = 4096
  def apply[F[_]: Concurrent: Timer: ContextShift, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]],
      pool: BlockingContext)
      : Resource[F, TimestampedStore[F, K, V]] = {
    val res: F[Resource[F, TimestampedStore[F, K, V]]] = for {
      entries <- underlying.entries.compile.toList.map(_.toMap)
      q <- Queue.bounded[F, (K, Timestamped[V])](QueueSize)
      r <- Ref.of[F, Map[K, Long]](entries.map {
        case (k, v) => (k, timestamp(v))
      })
      store <- Concurrent[F].delay(new TimestampedStore(underlying, r, q))
    } yield {
      val storeStream = Stream.emit[F, TimestampedStore[F, K, V]](store)
      storeStream.concurrently(store.updates).compile.resource.lastOrError
    }
    Resource.liftF(res).flatten.mapK[F](new ~>[F, F] {
      def apply[A](fa: F[A]): F[A] = ContextShift[F].evalOn(pool.unwrap)(fa)
    })
  }
}
