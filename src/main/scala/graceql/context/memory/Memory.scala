package graceql.context.memory

import graceql.core.*
import graceql.context.memory.Compiler
import graceql.data.*
import scala.collection.IterableFactory
import java.util.concurrent.LinkedBlockingQueue


sealed trait VoidSeq[+A] extends Iterable[A]
private object VoidSeq extends IterableFactory[VoidSeq]:
  private def exception = new GraceException(s"IterableFactory methods for the phantom collection type ${VoidSeq.getClass().getSimpleName} must not be called")
  def empty[A]: VoidSeq[A] = throw exception
  def from[A](source: IterableOnce[A]): VoidSeq[A] = throw exception
  def newBuilder[A]: scala.collection.mutable.Builder[A, VoidSeq[A]] = throw exception

class Ref[A](private val initial: A):
  private val lbq: LinkedBlockingQueue[A] = LinkedBlockingQueue(1)
  lbq.add(initial)

  protected [memory] def read(): A = 
    lbq.element()
  protected [memory] def write(v: A): Unit =
    lbq.clear()
    lbq.put(v)
  protected [memory] def write(f: A => A) = 
    val e = f(lbq.peek())
    lbq.clear()
    lbq.put(e)
  override def toString(): String = s"Ref(${read()})"  

final type IterRef[S[X] <: Iterable[X]] = [x] =>> Ref[S[x]]    
final type Eval[A] = IterRef[VoidSeq][A]

object Ref:

  opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]
  object IterableFactoryWrapper:
    given IterableFactoryWrapper[Seq] = Seq
    given IterableFactoryWrapper[List] = List
    given IterableFactoryWrapper[Vector] = Vector
    given IterableFactoryWrapper[LazyList] = LazyList
    given IterableFactoryWrapper[VoidSeq] = VoidSeq

  given memorySqlLike[S[X] <: Iterable[X], SR[X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S], ifac2: IterableFactoryWrapper[SR]): SqlLike[IterRef[SR], S] with { self =>
    private type M[A] = Source[IterRef[SR], S, A]

    extension [A](ma: M[A])
      private def merge: S[A] = ma match
          case Source.Values(c) => c
          case Source.Ref(mem)  => ifac.from(mem.read())
      private inline def mapValues[B](f: S[A] => S[B]) =
        Source.Values(ma.withValues(f))
      private inline def withValues[B](f: S[A] => B): B = f(ma.merge)

    type WriteResult = Unit

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
      @scala.annotation.nowarn def read: Read[IterRef[SR], S, A] =
        a match
          case s: (k, g) => (s._1, s._2.read)
          case s: M[a] => ifac.from(s.merge.map(_.read))
          case s: _ => s


    extension [A](ref: IterRef[SR][A])
      def insertMany(a: M[A]): Unit = 
        ref.write(s => ifac2.from(s ++ a.merge))
      def update(predicate: A => Boolean)(f: A => A): Unit = 
        ref.write(s => ifac2.from(s.map(e => if predicate(e) then f(e) else e)))
      def delete(predicate: A => Boolean): Unit =
        ref.write(s => ifac2.from(s.filterNot(predicate)))
  }

  given memoryContext[S[X] <: Iterable[X], SR[X] <: Iterable[X]](using sl: SqlLike[IterRef[SR],S]): Context[IterRef[SR], S] with
    type Compiled[A] = () => A

    type Connection = DummyImplicit
    inline def compile[A](inline query: SqlLike[IterRef[SR], S] ?=> A): () => A =
      ${ Compiler.compile[A]('{query(using sl)}) }

  given execSync[A, SR[X] <: Iterable[X]]: Execute[IterRef[SR], [x] =>> () => x, DummyImplicit, A, A] with
    def apply(compiled: () => A, conn: DummyImplicit): A = compiled()
