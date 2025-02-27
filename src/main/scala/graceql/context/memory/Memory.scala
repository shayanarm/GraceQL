package graceql.context.memory

import graceql.core.*
import graceql.context.memory.Compiler
import graceql.data.*
import graceql.syntax.*
import scala.quoted.*
import scala.collection.IterableFactory
import scala.collection.mutable.ArrayBuffer
import scala.util.Try



trait MemoryQueryContextProvider[R[_]]:
  opaque type IterableFactoryWrapper[S[X] <: Iterable[X]] = IterableFactory[S]
  object IterableFactoryWrapper:
    given IterableFactoryWrapper[Seq] = Seq
    given IterableFactoryWrapper[IndexedSeq] = IndexedSeq
    given IterableFactoryWrapper[List] = List
    given IterableFactoryWrapper[Vector] = Vector
    given IterableFactoryWrapper[LazyList] = LazyList
    given IterableFactoryWrapper[Iterable] = Iterable

  protected def refToIterable[A, S[_]](ref: R[A])(using ifac: IterableFactory[S]): S[A]
  protected def refInsertMany[A](ref: R[A])(as: Iterable[A]): Iterable[A]
  protected def refUpdate[A](ref: R[A])(predicate: A => Boolean)(f: A => A): Int
  protected def refDropWhile[A](ref: R[A])(predicate: A => Boolean): Int
  protected def refClear[A](ref: R[A]): Int
    
  inline def notSupported[A](): A = throw GraceException("Unsupported operation. The call to this method should be prevented during compile time.")

  extension [S[+X] <: Iterable[X], A](ma: Source[R, S, A])(using ifac: IterableFactoryWrapper[S])
      private def merge: S[A] = ma match
          case Source.Values(c) => c
          case Source.Ref(mem)  => refToIterable[A, S](mem)

  given memoryQueryable[S[+X] <: Iterable[X]](using ifac: IterableFactoryWrapper[S]): Queryable[R, S] with {
    type Src[A] = Source[R, S, A]

    protected def applyFun[T <: Tuple, B](name: String, args: T): B = notSupported()

    extension(sc: StringContext)
      def native(s: Any*): Any = notSupported()

    extension [A](ma: Src[A])
      private inline def mapValues[B](f: S[A] => S[B]): Src[B] =
        Source.Values(ma.withValues(f))
      private def withValues[B](f: S[A] => B): B = f(ma.merge)
      def size: Int = ma.merge.size
      override def map[B](f: A => B): Src[B] =
        ma.mapValues(v => ifac.from(v.map(f)))

      def flatMap[B](f: A => Src[B]): Src[B] =
        ma.mapValues { vs =>
          vs.flatMap { a =>
            f(a).withValues(identity)
          } |> ifac.from
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

      def leftJoin[B](mb: Src[B])(on: (A, B) => Boolean): Src[(A, Option[B])] = 
        for
          a <- ma
          ob <- mb.filter(b => on(a, b)) match
            case r if r.size == 0 => pure(None)
            case r => r.map(Some(_))
        yield (a, ob)

      override def rightJoin[B](mb: Src[B])(on: (A, B) => Boolean): Src[(Option[A], B)] = 
        for
          b <- mb
          oa <- ma.filter(a => on(a, b)) match
            case r if r.size == 0 => pure(None)
            case r => r.map(Some(_))
        yield (oa, b)

      def fullJoin[B](mb: Src[B])(on: (A, B) => Boolean): Src[Ior[A, B]] = ???

      def distinct: Src[A] = ma.mapValues(vs => ifac.from(vs.to(Seq).distinct))

      def groupBy[K](f: A => K): Src[(K, Src[A])] =
        ma.mapValues(vs => ifac.from(vs.groupBy(f).map[(K, Src[A])]((k,v) => (k, ifac.from(v).asSource))))

    extension [A](a: A) def pure: Src[A] = ifac(a).asSource

    extension [A](ref: R[A])
      override def insertMany[B](a: Src[A])(returning: A => B): S[B] = 
        ifac.from(refInsertMany(ref)(a.merge).map(returning))

      override def insertMany(a: Src[A]): Unit = 
        refInsertMany(ref)(a.merge)
      def insert[B](a: A)(returning: A => B): B = 
        insertMany(a.pure)(returning).head
      inline def insert(a: A): Unit = insert(a)(_ => ())
      def update(predicate: A => Boolean)(f: A => A): Int = 
        refUpdate(ref)(predicate)(f)
      def dropWhile(predicate: A => Boolean): Int =
        refDropWhile(ref)(predicate)
      override def clear(): Int = 
        refClear(ref) 

      def create() : Unit = notSupported()
      def delete() : Unit = notSupported()
  }

  given memoryQueryContext[S[+X] <: Iterable[X]](using sl: Queryable[R, S], ifac: IterableFactoryWrapper[S]): QueryContext[R, S] with
    type Native[A] = () => A
    type Connection = DummyImplicit

    @scala.annotation.nowarn private def read[A](a: A): graceql.core.Read[R, S, A] =
      a match
        case s: (k, g) => (s._1, read(s._2))
        case s: Source[R, S, x] => ifac.from(s.merge.map(read))
        case _ => a              

    inline def compile[A](inline query: Queryable ?=> A): () => graceql.core.Read[R, S, A] =
      ${ Compiler.compile[R, S, graceql.core.Read[R, S, A]]('{read[A](query(using sl))}) }

    inline def tryCompile[A](inline query: Queryable ?=> A): Try[() => graceql.core.Read[R, S, A]] =
      ${ Compiler.tryCompile[R, S, graceql.core.Read[R, S, A]]('{read[A](query(using sl))}) }
      

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

  protected [memory] inline def dropWhile(pred: A => Boolean): Int = write {buffer => 
    val l = buffer.length
    buffer.dropWhileInPlace(pred)
    l - buffer.length
  }
  protected [memory] inline def clear(): Int = write {buffer => 
    val l = buffer.length
    buffer.clear()
    l
  }      

object IterRef extends MemoryQueryContextProvider[IterRef]:
  
  protected def refToIterable[A, S[_]](ref: IterRef[A])(using ifac: IterableFactory[S]): S[A] = 
    ref.value[S]
  
  protected def refInsertMany[A](ref: IterRef[A])(as: Iterable[A]): Iterable[A] = 
    ref.insertMany(as)
    as
  
  protected def refUpdate[A](ref: IterRef[A])(predicate: A => Boolean)(f: A => A): Int = 
    ref.update(predicate)(f)
  
  protected def refDropWhile[A](ref: IterRef[A])(predicate: A => Boolean): Int = 
    ref.dropWhile(predicate)

  protected def refClear[A](ref: IterRef[A]): Int = 
    ref.clear()  

final type Eval[A]
object Eval extends MemoryQueryContextProvider[Eval]:
  def absurd[A,B](ref: Eval[A]): B = throw new GraceException(s"Supplying a value for ${Eval.getClass.getSimpleName} is impossible!")
  protected def refToIterable[A,S[_]](ref: Eval[A])(using ifac: IterableFactory[S]): S[A] = absurd(ref)
  protected def refInsertMany[A](ref: Eval[A])(as: Iterable[A]): Iterable[A] = absurd(ref)
  protected def refUpdate[A](ref: Eval[A])(predicate: A => Boolean)(f: A => A): Int = absurd(ref)
  protected def refDropWhile[A](ref: Eval[A])(predicate: A => Boolean): Int = absurd(ref)
  protected def refClear[A](ref: Eval[A]): Int = absurd(ref)