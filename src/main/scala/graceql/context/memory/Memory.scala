package graceql.context.memory

import graceql.core.*
import graceql.context.memory.Compiler
import graceql.data.*
import scala.collection.IterableFactory
import scala.collection.mutable.ArrayBuffer

trait MemoryQueryContextImpl[R[_]]:
  opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]
  object IterableFactoryWrapper:
    given IterableFactoryWrapper[Seq] = Seq
    given IterableFactoryWrapper[List] = List
    given IterableFactoryWrapper[Vector] = Vector
    given IterableFactoryWrapper[LazyList] = LazyList
    given IterableFactoryWrapper[Iterable] = Iterable

  protected def refToIterable[A,S[_]](ref: R[A])(using ifac: IterableFactory[S]): S[A]
  protected def refInsertMany[A](ref: R[A])(as: Iterable[A]): Iterable[A]
  protected def refUpdate[A](ref: R[A])(predicate: A => Boolean)(f: A => A): Int
  protected def refDelete[A](ref: R[A])(predicate: A => Boolean): Int
  protected def refClear[A](ref: R[A]): Int

  given memoryQueryable[S[+X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S]): Queryable[R, S, [x] =>> () => x] with { self =>
    private type Src[A] = Source[R, S, A]

    extension [A](ma: Src[A])
      private def merge: S[A] = ma match
          case Source.Values(c) => c
          case Source.Ref(mem)  => refToIterable(mem)(using ifac)
      private inline def mapValues[B](f: S[A] => S[B]) =
        Source.Values(ma.withValues(f))
      private inline def withValues[B](f: S[A] => B): B = f(ma.merge)
  
    private def absurd(using NativeSupport[[x] =>> () => x]) = throw GraceException("`in-memory` contexts do not have `NativeSupport`")

    extension[A](bin: () => A)(using NativeSupport[[x] =>> () => x])
      def typed[B]: () => B = absurd
      def unlift: A = absurd
    extension[A](a: A)
      def lift: () => A = () => a

    extension(sc: StringContext)(using NativeSupport[[x] =>> () => x])
      def native(s: (Queryable[R,S,[x] =>> () => x] ?=> Any)*): () => Any = absurd

    extension [A](ma: Src[A])

      def size: Int = ma.merge.size
      override def map[B](f: A => B): Src[B] =
        ma.mapValues(v => ifac.from(v.map(f)))

      def flatMap[B](f: A => Src[B]): Src[B] =
        ma.mapValues { vs =>
          val r = vs.flatMap{a =>
            f(a).withValues(identity)
          }
          ifac.from(r)
        }

      def concat(other: Src[A]): Src[A] =
        other.withValues { vs => ma.mapValues(ss => ifac.from(ss.concat(vs)))}

      def filter(pred: A => Boolean): Src[A] = ma.mapValues(vs => ifac.from(vs.filter(pred)))

      def withFilter(pred: A => Boolean): scala.collection.WithFilter[A, Src] =
        val iterOps = new scala.collection.IterableOps[A,Src,Src[A]] { io =>
          def iterator = ma.withValues(_.iterator)
          def coll = ma
          protected def fromSpecific(coll: IterableOnce[A @scala.annotation.unchecked.uncheckedVariance]): Src[A] =
            ifac.from(coll).asSource
          def iterableFactory: IterableFactory[Src] =
            new IterableFactory[Src] {
                def from[X](source: IterableOnce[X]): Src[X] = ifac.from(source).asSource
                def empty[X]: Src[X] = ifac.empty[X].asSource
                def newBuilder[X]: scala.collection.mutable.Builder[X, Src[X]] =
                  val seqBuilder = ifac.newBuilder[X]
                  new scala.collection.mutable.Builder[X,Src[X]] {
                    def addOne(elem: X) =
                      seqBuilder.addOne(elem)
                      this
                    def clear(): Unit = seqBuilder.clear()
                    def result(): Src[X] = seqBuilder.result().asSource
                  }
            }
          protected def newSpecificBuilder: scala.collection.mutable.Builder[A @scala.annotation.unchecked.uncheckedVariance, Src[A]] =
            io.iterableFactory.newBuilder
          def toIterable = ma.withValues(_.toIterable)
        }
        new scala.collection.IterableOps.WithFilter[A,Src](iterOps,pred)

      def leftJoin[B](mb: Src[B])(on: (A, B) => Boolean): Src[(A, Option[B])] = ???

      def fullJoin[B](mb: Src[B])(on: (A, B) => Boolean): Src[Ior[A, B]] = ???

      def distinct: Src[A] = ma.mapValues(vs => ifac.from(vs.to(Seq).distinct))

      def groupBy[K](f: A => K): Src[(K, Src[A])] =
        ma.mapValues(vs => ifac.from(vs.groupBy(f).map[(K, Src[A])]((k,v) => (k, ifac.from(v).asSource))))

    extension [A](a: A) def pure: Src[A] = ifac(a).asSource

    extension [A](a: A)
      @scala.annotation.nowarn def read: Read[R, S, A] =
        a match
          case s: (k, g) => (s._1, s._2.read)
          case s: Src[a] => ifac.from(s.merge.map(_.read))
          case s: _ => a

    extension [A](ref: R[A])
      override def insertMany[B](a: Src[A])(returning: A => B): S[B] = 
        ifac.from(refInsertMany(ref)(a.merge).map(returning))

      override def insertMany(a: Src[A]): Unit = 
        refInsertMany(ref)(a.merge)
      def insert[B](a: A)(returning: A => B): B = 
        insertMany(a.pure)(returning).head
      def update(predicate: A => Boolean)(f: A => A): Int = 
        refUpdate(ref)(predicate)(f)
      def delete(predicate: A => Boolean): Int =
        refDelete(ref)(predicate)
      override def clear(): Int = 
        refClear(ref) 
  }

  given memoryQueryContext[S[+X] <: Iterable[X], R[_]](using sl: Queryable[R, S, [x] =>> () => x]): QueryContext[R, S] with
    type Native[A] = () => A
    type Connection = DummyImplicit
    inline def compile[A](inline query: Queryable ?=> A): () => A =
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

  protected [memory] def write[R](f: ArrayBuffer[A] => R): R = 
    synchronized {
      f(underlying)
    }
    
  protected [memory] inline def insertMany(as: Iterable[A]): Unit = write {_ ++= as}

  protected [memory] inline def update(pred: A => Boolean)(f: A => A): Int = write {buffer => 
    var nUpdated = 0
    buffer.indices.foreach {i => 
      val elem = buffer(i)
      if (pred(elem)) then 
        nUpdated +=1
        buffer.update(i, f(elem))  
    }
    nUpdated  
  }

  protected [memory] inline def delete(pred: A => Boolean): Int = write {buffer => 
    val l = buffer.length
    buffer.dropWhileInPlace(pred)
    l - buffer.length
  }
  protected [memory] inline def clear(): Int = write {buffer => 
    val l = buffer.length
    buffer.clear()
    l
  }      

object IterRef extends MemoryQueryContextImpl[IterRef]:
  
  protected def refToIterable[A, S[_]](ref: IterRef[A])(using ifac: IterableFactory[S]): S[A] = 
    ref.value[S]
  
  protected def refInsertMany[A](ref: IterRef[A])(as: Iterable[A]): Iterable[A] = 
    ref.insertMany(as)
    as
  
  protected def refUpdate[A](ref: IterRef[A])(predicate: A => Boolean)(f: A => A): Int = 
    ref.update(predicate)(f)
  
  protected def refDelete[A](ref: IterRef[A])(predicate: A => Boolean): Int = 
    ref.delete(predicate)

  protected def refClear[A](ref: IterRef[A]): Int = 
    ref.clear()  

final type Eval[A]
object Eval extends MemoryQueryContextImpl[Eval]:
  def absurd[A,B](ref: Eval[A]): B = throw new GraceException(s"Supplying a value for ${Eval.getClass.getSimpleName} is impossible!")
  protected def refToIterable[A,S[_]](ref: Eval[A])(using ifac: IterableFactory[S]): S[A] = absurd(ref)
  protected def refInsertMany[A](ref: Eval[A])(as: Iterable[A]): Iterable[A] = absurd(ref)
  protected def refUpdate[A](ref: Eval[A])(predicate: A => Boolean)(f: A => A): Int = absurd(ref)
  protected def refDelete[A](ref: Eval[A])(predicate: A => Boolean): Int = absurd(ref)
  protected def refClear[A](ref: Eval[A]): Int = absurd(ref)