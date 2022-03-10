package graceql.backend.memory

import graceql.core.*
import graceql.backend.memory.Compiler
import graceql.data.*
import scala.reflect.TypeTest
import scala.collection.IterableFactory

final type memory[+A]

object memory {

  extension [M[_], A](ma: Source[memory, M, A])
    private inline def mapValues[B](f: M[A] => M[B]) =
      Source.Values(f(ma.asInstanceOf[Source.Values[M, A]].values))
    private inline def withValues[B](f: M[A] => B): B = f(
      ma.asInstanceOf[Source.Values[M, A]].values
    )

  given SqlLike[memory, Seq] with { self =>
    private type M[A] = Source[memory, Seq, A]
    extension [A](ma: M[A])
      override def map[B](f: A => B): M[B] =
        ma.mapValues(_.map(f))

      // override def ap[B](f: M[A => B]): M[B] = ???

      def flatMap[B](f: A => M[B]): M[B] =
        ma.mapValues { vs =>
          vs.flatMap{a =>
            f(a).withValues(identity)  
          }
        }

      def concat(other: M[A]): M[A] =
        other.withValues { vs => ma.mapValues(_.concat(vs))}

      def filter(pred: A => Boolean): M[A] = ma.mapValues(_.filter(pred))

      def withFilter(pred: A => Boolean): scala.collection.WithFilter[A, M] =
        val iterOps = new scala.collection.IterableOps[A,M,M[A]] { io =>
          def iterator = ma.withValues(_.iterator)
          def coll = ma
          protected def fromSpecific(coll: IterableOnce[A @scala.annotation.unchecked.uncheckedVariance]): M[A] =
            coll.iterator.to(Seq).asSource
          def iterableFactory: IterableFactory[M] =
            new IterableFactory[M] {
                def from[X](source: IterableOnce[X]): M[X] = Seq.from(source).asSource
                def empty[X]: M[X] = Seq.empty[X].asSource
                def newBuilder[X]: scala.collection.mutable.Builder[X, M[X]] = 
                  val seqBuilder = Seq.newBuilder[X] 
                  new scala.collection.mutable.Builder[X,M[X]] { 
                    def addOne(elem: X) = 
                      seqBuilder.addOne(elem)
                      this               
                    def clear(): Unit = seqBuilder.clear()
                    def result(): M[X] = seqBuilder.result().asSource
                  }                  
            }
          protected def newSpecificBuilder: scala.collection.mutable.Builder[A @scala.annotation.unchecked.uncheckedVariance, M[A]] = 
            io.iterableFactory.newBuilder
          def toIterable = ma.withValues(_.toIterable) 
        }
        new scala.collection.IterableOps.WithFilter[A,M](iterOps,pred)

      def leftJoin[B](mb: M[B], on: (A, B) => Boolean): M[(A, Option[B])] = ???

      def fullJoin[B](mb: M[B], on: (A, B) => Boolean): M[Ior[A, B]] = ???

      def distinct: M[A] = ma.mapValues(_.distinct)

      def groupBy[K](f: A => K): M[(K, M[A])] = ???

    extension [A](a: A) def pure: M[A] = Seq(a).asSource

    extension [A](a: A)(using tt: TypeTest[A, Read[A]])
      def read: Read[A] = a match
        case tt(a) => a
        case _ =>
          a.asInstanceOf[Source.Values[Seq, ?]].values.asInstanceOf[Read[A]]

    extension [A](ref: memory[A])
      def insertMany(a: M[A]): Int = ???
      def update(predicate: A => Boolean)(a: A => A): Int = ???
      def delete(a: A => Boolean): Int = ???
  }

  given memoryCtx[S[_]](using SqlLike[memory,S]): Context[memory,S] with
    transparent inline def apply[A](inline query: SqlLike[memory, S] ?=> A): A =
        ${ Compiler.compile('{ query(using summon[SqlLike[memory, S]]) }) }
}