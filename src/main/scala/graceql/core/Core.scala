package graceql.core

import graceql.data.*
import scala.annotation.targetName
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.compiletime.summonInline
import scala.concurrent.Promise
import scala.util.Try

class GraceException(val message: Option[String] = None, val cause: Option[Throwable] = None)
    extends Exception(message.orNull, cause.orNull):
  def this(message: String, cause: Throwable) = this(Some(message), Some(cause))
  def this(message: String) = this(Some(message), None)
  def this(cause: Throwable) = this(None, Some(cause))

class terminal extends scala.annotation.StaticAnnotation

trait Execute[R[_], Binary[_], Connection, A, B]:
  def apply(compiled: Binary[A], conn: Connection): B

object Execute:
  given execLifted[R[_], Binary[_], Connection, A, B, G[_]](using
      execUnlifted: Execute[R, Binary, Connection, A, B],
      run: RunLifted[G]
  ): Execute[R, Binary, Connection, A, G[B]] with
    def apply(compiled: Binary[A], conn: Connection): G[B] =
      run(() => execUnlifted(compiled, conn))

class Exe[R[_], Binary[_], Connection, A](val compiled: Binary[A]):
  inline def apply[B](using conn: Connection): B =
    summonInline[Execute[R, Binary, Connection, A, B]].apply(compiled, conn)  
  inline def as[C[_]](using Connection): C[A] =
    apply[C[A]]
  inline def run(using Connection): A = as[[x] =>> x]
  inline def future(using Connection): Future[A] = as[Future]
  inline def promise(using Connection): Promise[A] = as[Promise]
  inline def asTry(using Connection): Try[A] = as[Try]
  inline def option(using Connection): Option[A] = as[Option]
  inline def either(using Connection): Either[Throwable, A] =
    as[[x] =>> Either[Throwable, x]]  

trait Capabilities[C[_]]:
  def fromBinary[A](bin: C[A]): A
  protected def toBinary[A](a: A): C[A]
  object function:
    inline def nullary[A](inline f: C[A]): () => A = 
      () => fromBinary(f)
    inline def unary[A, B](inline f: C[A] => C[B]): A => B = 
      a => fromBinary(f(toBinary(a)))
    inline def binary[A1, A2, B](inline f: (C[A1], C[A2]) => C[B]): (A1, A2) => B = 
      (a1, a2) => fromBinary(f(toBinary(a1), toBinary(a2)))
    inline def ternary[A1, A2, A3, B](inline f: (C[A1], C[A2], C[A3]) => C[B]): (A1, A2, A3) => B =
      (a1, a2, a3) => fromBinary(f(toBinary(a1), toBinary(a2), toBinary(a3)))
    inline def quarternary[A1, A2, A3, A4, B](inline f: (C[A1], C[A2], C[A3], C[A4]) => C[B]): (A1, A2, A3, A4) => B =
      (a1, a2, a3, a4) => fromBinary(f(toBinary(a1), toBinary(a2), toBinary(a3), toBinary(a4)))

trait Context[R[_]]:
  self =>
  type Binary[A]
  type Capabilities <: graceql.core.Capabilities[Binary]
  type Connection

  final type Execute[A, B] = graceql.core.Execute[R, Binary, Connection, A, B]

  type Exe[A] <: graceql.core.Exe[R, Binary, Connection, A]

  inline def exe[A](compiled: Binary[A]): Exe[A]
  
  inline def apply[A](inline query: Capabilities ?=> A): Exe[A] =
    exe(compile(query))

  inline def compile[A](inline query: Capabilities ?=> A): Binary[A]

final type Read[R[_], M[_], T] = T match
  case (k, grpd)       => (k, Read[R, M, grpd])
  case Source[R, M, a] => M[Read[R, M, a]]
  case _               => T

trait Queryable[R[_], M[+_], C[_]] extends SQLLike[[x] =>> Source[R, M, x]] with Capabilities[C]:

  extension [A](a: A)
    @terminal
    def read: Read[R, M, A]
  extension [A](values: M[A])
    @targetName("valuesAsSource")
    inline def asSource: Source[R, M, A] = Source.Values(values)
  extension [A](ref: R[A])
    @targetName("refAsSource")
    inline def asSource: Source[R, M, A] = Source.Ref(ref)
    @terminal
    def insertMany[B](a: Source[R, M, A])(returning: A => B): M[B]
    @terminal
    def insertMany(a: Source[R, M, A]): Unit
    @terminal
    inline def ++=(a: Source[R, M, A]): Unit = insertMany(a)
    @terminal
    def insert[B](a: A)(returning: A => B): B
    @terminal
    inline def insert[B](a: A): Unit = insert(a)(a => ())
    @terminal
    inline def +=(a: A): Unit = insert(a)
    @terminal
    def update(predicate: A => Boolean)(f: A => A): Int
    @terminal
    def delete(predicate: A => Boolean): Int
    @terminal
    inline def dropWhile(predicate: A => Boolean): Int = delete(predicate)
    @terminal
    def clear(): Int = delete(_ => true)
    @terminal
    inline def truncate(): Int = clear()  

trait QueryContext[R[_], M[+_]] extends Context[R]:
  self =>

  final type Queryable = graceql.core.Queryable[R, M, Binary]
  final type Capabilities = Queryable
  final class Exe[A](compiled: Binary[A]) extends graceql.core.Exe[R, Binary, Connection, A](compiled):
    type RowType = A match
      case M[a] => a
      case _    => A
    inline def transform[D[_]](using Connection)(using
        eq: A =:= M[RowType]
    ): D[RowType] =
      // apply[M[RowType], D[RowType]](eq.liftCo[Binary](compiled))
      apply[D[RowType]]
    inline def lazyList(using Connection)(using
        A =:= M[RowType]
    ): LazyList[RowType] =
      transform[LazyList]
    inline def stream(using Connection)(using
        A =:= M[RowType]
    ): LazyList[RowType] =
      lazyList
  inline def exe[A](compiled: Binary[A]): Exe[A] = Exe(compiled)      

trait Definable[R[_], C[_]] extends Capabilities[C]:
  extension[A](ref: R[A])
    def create(): Unit  
    def drop(): Unit
      
trait SchemaContext[R[_]] extends Context[R]:
  self =>

  final type Definable = graceql.core.Definable[R, Binary]
  final type Capabilities = Definable
  final type Exe[A] = graceql.core.Exe[R, Binary, Connection, A]
  inline def exe[A](compiled: Binary[A]): Exe[A] =
    graceql.core.Exe[R,Binary,Connection,A](compiled)

trait ACID[C]:
  def session(connection: C): C
  def open(connection: C): Unit
  def commit(connection: C): Unit
  def rollback(connection: C): Unit

object ACID:
end ACID

sealed class Transaction[T[_], C, A]
object Transaction:
  case class Conclusion[T[_], A](val run: () => T[A])
      extends Transaction[T, Nothing, A]
  case class Continuation[T[_], C](
      protected val sessionFactory: () => C,
      protected val open: C => T[Unit],
      protected val commit: C => T[Unit],
      protected val rollback: C => T[Unit],
      protected val me: MonadError[T]
  ) extends Transaction[T, C, Nothing]:
    lazy val session = sessionFactory()

  extension [C](connection: C)
    def transaction[T[_]](using
        acid: ACID[C],
        run: RunLifted[T],
        me: MonadError[T]
    ): Transaction[T, C, Nothing] =
      Transaction.Continuation(
        () => acid.session(connection),
        c => run(() => acid.open(c)),
        c => run(() => acid.commit(c)),
        c => run(() => acid.rollback(c)),
        me
      )
  extension [T[_], C](tr: Transaction[T, C, Nothing])
    @scala.annotation.nowarn final def apply[A](block: C ?=> T[A]): T[A] = tr match
      case conn @ Continuation(_, open, commit, rollback, me) =>
        given MonadError[T] = me
        for
          _ <- open(conn.session)
          thunk = for
            r <- block(using conn.session)
            _ <- commit(conn.session)
          yield r
          r <- thunk.recoverWith { e =>
            for
              _ <- rollback(conn.session)
              a <- me.raiseError[A](e)
            yield a
          }
        yield r
    final def map[A](f: C => T[A]): Transaction[T, Nothing, A] =
      Conclusion(() => apply(s ?=> f(s)))
    @scala.annotation.nowarn final def withFilter(pred: C => Boolean): Transaction[T, C, Nothing] =
      tr match
        case conn @ Continuation(_, _, _, _, me) =>
          pred(conn.session) match
            case true => tr
            case false =>
              throw new NoSuchElementException(
                "Transaction.withFilter predicate is not satisfied. Also, this method should not be called"
              )
    @scala.annotation.nowarn final def flatMap[C2, A](
        f: C => Transaction[T, C2, A]
    ): Transaction[T, C2, A] = tr match
      case conn @ Continuation(_, o1, c1, rb1, me) =>
        given MonadError[T] = me
        f(conn.session) match
          case Conclusion(thunk) =>
            Conclusion(() => apply(_ ?=> thunk()))
          case Continuation(f2, o2, c2, rb2, _) =>
            Continuation(
              f2,
              s2 =>
                for
                  _ <- o1(conn.session)
                  _ <- o2(s2)
                yield (),
              s2 =>
                for
                  _ <- c2(s2)
                  _ <- c1(conn.session)
                yield (),
              s2 =>
                for
                  _ <- rb2(s2)
                  _ <- rb1(conn.session)
                yield (),
              me
            )
  extension [T[_], A](tr: Transaction[T, Nothing, A])
    def run(): T[A] = tr.asInstanceOf[Transaction.Conclusion[T, A]].run()
