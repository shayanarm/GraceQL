package graceql.context.memory

import graceql.core.*
import graceql.context.memory.Compiler
import graceql.data.*
import scala.collection.IterableFactory
import scala.collection.mutable.ArrayBuffer

trait MemoryContextImpl[R[_]]:
  opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]
  object IterableFactoryWrapper:
    given IterableFactoryWrapper[Seq] = Seq
    given IterableFactoryWrapper[List] = List
    given IterableFactoryWrapper[Vector] = Vector
    given IterableFactoryWrapper[LazyList] = LazyList
    given IterableFactoryWrapper[Iterable] = Iterable

  protected def refToIterable[A,S[_]](ref: R[A])(using ifac: IterableFactory[S]): S[A]
  protected def refInsertMany[A](ref: R[A])(as: Iterable[A]): Unit
  protected def refUpdate[A](ref: R[A])(predicate: A => Boolean)(f: A => A): Unit
  protected def refDelete[A](ref: R[A])(predicate: A => Boolean): Unit
  protected def refClear[A](ref: R[A]): Unit

  given memoryQueryable[S[X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S]): Queryable[R, S, Unit] with { self =>
    private type M[A] = Source[R, S, A]

    extension [A](ma: M[A])
      private def merge: S[A] = ma match
          case Source.Values(c) => c
          case Source.Ref(mem)  => refToIterable(mem)(using ifac)
      private inline def mapValues[B](f: S[A] => S[B]) =
        Source.Values(ma.withValues(f))
      private inline def withValues[B](f: S[A] => B): B = f(ma.merge)

    type WriteResult = Unit

    extension [A](ma: M[A])

      def size: Int = ma.merge.size
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
          case s: _ => a

    extension [A](ref: R[A])
      def insertMany(a: M[A]): Unit = 
        refInsertMany(ref)(a.merge)
      def update(predicate: A => Boolean)(f: A => A): Unit = 
        refUpdate(ref)(predicate)(f)
      def delete(predicate: A => Boolean): Unit =
        refDelete(ref)(predicate)
      override def clear(): Unit = 
        refClear(ref) 
  }

  given memoryContext[S[X] <: Iterable[X], R[_]](using sl: Queryable[R, S, Unit]): Context[R, S] with
    type Compiled[A] = () => A
    type WriteResult = Unit
    type Connection = DummyImplicit
    inline def compile[A](inline query: Queryable[R, S, Unit] ?=> A): () => A =
      ${ Compiler.compile[A]('{query(using sl)}) }

  given execSync[A,R[_]]: Execute[R, [x] =>> () => x, DummyImplicit, A, A] with
    def apply(compiled: () => A, conn: DummyImplicit): A = compiled()

class IterRef[A] private(private val underlying: ArrayBuffer[A]):
  def this(initial: Iterable[A]) = this(ArrayBuffer.from(initial))
  def this(values: A*) = this(values)

  protected [memory] def value[S[_]](using ifac: IterableFactory[S]): S[A] = 
    synchronized {
      ifac.from(underlying)
    }

  protected [memory] def write(f: ArrayBuffer[A] => Unit): Unit = 
    synchronized {
      f(underlying)
    }
    
  protected [memory] inline def insertMany(as: Iterable[A]): Unit = write {_ ++= as}

  protected [memory] inline def update(pred: A => Boolean)(f: A => A): Unit = write {buffer => 
    buffer.indices.foreach {i => 
      val elem = buffer(i)
      if (pred(elem)) then 
        buffer.update(i, f(elem))  
    }
  }

  protected [memory] inline def delete(pred: A => Boolean): Unit = write {_.dropWhileInPlace(pred)}
  protected [memory] inline def clear(): Unit = write {_.clear()}      

object IterRef extends MemoryContextImpl[IterRef]:
  
  protected def refToIterable[A, S[_]](ref: IterRef[A])(using ifac: IterableFactory[S]): S[A] = 
    ref.value[S]
  
  protected def refInsertMany[A](ref: IterRef[A])(as: Iterable[A]): Unit = 
    ref.insertMany(as)
  
  protected def refUpdate[A](ref: IterRef[A])(predicate: A => Boolean)(f: A => A): Unit = 
    ref.update(predicate)(f)
  
  protected def refDelete[A](ref: IterRef[A])(predicate: A => Boolean): Unit = 
    ref.delete(predicate)

  protected def refClear[A](ref: IterRef[A]): Unit = 
    ref.clear()  

final type Eval[A]
object Eval extends MemoryContextImpl[Eval]:
  def absurd[A,B](ref: Eval[A]): B = throw new GraceException(s"Supplying a value for ${Eval.getClass.getSimpleName} is impossible!")
  protected def refToIterable[A,S[_]](ref: Eval[A])(using ifac: IterableFactory[S]): S[A] = absurd(ref)
  protected def refInsertMany[A](ref: Eval[A])(as: Iterable[A]): Unit = absurd(ref)
  protected def refUpdate[A](ref: Eval[A])(predicate: A => Boolean)(f: A => A): Unit = absurd(ref)
  protected def refDelete[A](ref: Eval[A])(predicate: A => Boolean): Unit = absurd(ref)
  protected def refClear[A](ref: Eval[A]): Unit = absurd(ref)