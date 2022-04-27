package graceql.context.memory

import graceql.core.*
import graceql.context.memory.Compiler
import graceql.data.*
import scala.collection.IterableFactory

trait MemoryContextImpl[R[_]]:
  opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]
  object IterableFactoryWrapper:
    given IterableFactoryWrapper[Seq] = Seq
    given IterableFactoryWrapper[List] = List
    given IterableFactoryWrapper[Vector] = Vector
    given IterableFactoryWrapper[LazyList] = LazyList
    given IterableFactoryWrapper[Iterable] = Iterable

  protected def refToIterable[A](ref: R[A]): Iterable[A]
  protected def refInsertMany[A, S[X] <: Iterable[X]](ref: R[A])(as: S[A]): Unit
  protected def refUpdate[A](ref: R[A])(predicate: A => Boolean)(f: A => A): Unit
  protected def refDelete[A](ref: R[A])(predicate: A => Boolean): Unit

  given memorySqlLike[S[X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S]): SqlLike[R, S] with { self =>
    private type M[A] = Source[R, S, A]

    extension [A](ma: M[A])
      private def merge: S[A] = ma match
          case Source.Values(c) => c
          case Source.Ref(mem)  => ifac.from(refToIterable(mem))
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
      @scala.annotation.nowarn def read: Read[R, S, A] =
        a match
          case s: (k, g) => (s._1, s._2.read)
          case s: M[a] => ifac.from(s.merge.map(_.read))
          case s: _ => s


    extension [A](ref: R[A])
      def insertMany(a: M[A]): Unit = 
        refInsertMany(ref)(a.merge)
      def update(predicate: A => Boolean)(f: A => A): Unit = 
        refUpdate(ref)(predicate)(f)
      def delete(predicate: A => Boolean): Unit =
       refDelete(ref)(predicate)
  }

  given memoryContext[S[X] <: Iterable[X], R[_]](using sl: SqlLike[R, S]): Context[R, S] with
    type Compiled[A] = () => A

    type Connection = DummyImplicit
    inline def compile[A](inline query: SqlLike[R, S] ?=> A): () => A =
      ${ Compiler.compile[A]('{query(using sl)}) }

  given execSync[A,R[_]]: Execute[R, [x] =>> () => x, DummyImplicit, A, A] with
    def apply(compiled: () => A, conn: DummyImplicit): A = compiled()

class IterRef[A](private var underlying: Iterable[A]):
  def this(values: A*) = this(values)
  protected [memory] def value: Iterable[A] = 
    underlying
  protected [memory] def value(f: Iterable[A] => Iterable[A]): Unit = 
    synchronized {
      underlying = f(underlying)
    }

  override def toString(): String = s"IterRef(${value})"        

object IterRef extends MemoryContextImpl[IterRef]:
  
  protected def refToIterable[A](ref: IterRef[A]): Iterable[A] = ref.value
  
  protected def refInsertMany[A, S[X] <: Iterable[X]](ref: IterRef[A])(as: S[A]): Unit = 
    ref.value(s => s ++ as)
  
  protected def refUpdate[A](ref: IterRef[A])(predicate: A => Boolean)(f: A => A): Unit = 
    ref.value(s => s.map(e => if predicate(e) then f(e) else e))
  
  protected def refDelete[A](ref: IterRef[A])(predicate: A => Boolean): Unit = 
    ref.value(s => s.filterNot(predicate))

final type Eval[A]
object Eval extends MemoryContextImpl[Eval]:
  def absured[A,B](ref: Eval[A]): B = throw new GraceException(s"Supplying a value for ${Eval.getClass.getSimpleName} is impossible!")
  protected def refToIterable[A](ref: Eval[A]): Iterable[A] = absured(ref)
  protected def refInsertMany[A, S[X] <: Iterable[X]](ref: Eval[A])(as: S[A]): Unit = absured(ref)
  protected def refUpdate[A](ref: Eval[A])(predicate: A => Boolean)(f: A => A): Unit = absured(ref)
  protected def refDelete[A](ref: Eval[A])(predicate: A => Boolean): Unit = absured(ref)