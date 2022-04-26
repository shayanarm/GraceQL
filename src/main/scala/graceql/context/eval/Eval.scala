package graceql.context.eval

import graceql.core.*
import graceql.context.eval.Compiler
import graceql.data.*
import scala.collection.IterableFactory

final type Eval[+A]

opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]

object IterableFactoryWrapper {
  implicit val seqFactory: IterableFactoryWrapper[Seq] = Seq
  implicit val listFactory: IterableFactoryWrapper[List] = List
  implicit val vectorFactory: IterableFactoryWrapper[Vector] = Vector  
  implicit val lazyListFactory: IterableFactoryWrapper[LazyList] = LazyList  
}

object Eval {

  extension [M[_], A](ma: Source[Eval, M, A])
    private inline def mapValues[B](f: M[A] => M[B]) =
      Source.Values(ma.withValues(f))
    private inline def withValues[B](f: M[A] => B): B = f(
      ma.asInstanceOf[Source.Values[M, A]].values
    )

  given EvalIterable[S[X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S]): SqlLike[Eval, S] with { self =>
    private type M[A] = Source[Eval, S, A]

    type WriteResult = Int

    extension [A](ma: M[A])
      override def map[B](f: A => B): M[B] =
        ma.mapValues(v => ifac.from(v.map(f)))

      def flatMap[B](f: A => M[B]): M[B] =
        ma.mapValues { vs =>
          val r = vs.flatMap{a =>
            f(a).withValues(identity)  
          }
          ifac.from(r)
        }

      def concat(other: M[A]): M[A] =
        other.withValues { vs => ma.mapValues(ss => ifac.from(ss.concat(vs)))}

      def filter(pred: A => Boolean): M[A] = ma.mapValues(vs => ifac.from(vs.filter(pred)))

      def withFilter(pred: A => Boolean): scala.collection.WithFilter[A, M] =
        val iterOps = new scala.collection.IterableOps[A,M,M[A]] { io =>
          def iterator = ma.withValues(_.iterator)
          def coll = ma
          protected def fromSpecific(coll: IterableOnce[A @scala.annotation.unchecked.uncheckedVariance]): M[A] =
            ifac.from(coll).asSource
          def iterableFactory: IterableFactory[M] =
            new IterableFactory[M] {
                def from[X](source: IterableOnce[X]): M[X] = ifac.from(source).asSource
                def empty[X]: M[X] = ifac.empty[X].asSource
                def newBuilder[X]: scala.collection.mutable.Builder[X, M[X]] = 
                  val seqBuilder = ifac.newBuilder[X]
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

      def leftJoin[B](mb: M[B])(on: (A, B) => Boolean): M[(A, Option[B])] = ???

      def fullJoin[B](mb: M[B])(on: (A, B) => Boolean): M[Ior[A, B]] = ???

      def distinct: M[A] = ma.mapValues(vs => ifac.from(vs.to(Seq).distinct))

      def groupBy[K](f: A => K): M[(K, M[A])] = 
        ma.mapValues(vs => ifac.from(vs.groupBy(f).map[(K, M[A])]((k,v) => (k, ifac.from(v).asSource))))

    extension [A](a: A) def pure: M[A] = ifac(a).asSource
     
    extension [A](a: A) 
      @scala.annotation.nowarn def read: Read[A] = a match
        case s: (k, g) => (s._1, s._2.read)
        case s: Source[Eval, S, x] => ifac.from(s.asInstanceOf[Source.Values[S,x]].values.map(_.read))
        case s: _ => s

    extension [A](ref: Eval[A])
      def insertMany(a: M[A]): Int = ???
      def update(predicate: A => Boolean)(a: A => A): Int = ???
      def delete(a: A => Boolean): Int = ???
  }

  given evalContext[S[X] <: Iterable[X]](using sl: SqlLike[Eval,S]): Context[Eval,S] with
    type Compiled[A] = () => A

    type Connection = DummyImplicit
    inline def compile[A](inline query: SqlLike[Eval, S] ?=> A): () => A = 
      ${ Compiler.compile[A]('{query(using sl)}) }

  given execSync[A]: Execute[Eval, [x] =>> () => x, DummyImplicit, A, A] with
    def apply(compiled: () => A, conn: DummyImplicit): A = compiled()
}