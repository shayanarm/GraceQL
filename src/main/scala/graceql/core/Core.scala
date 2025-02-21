package graceql.core

import graceql.data.*
import scala.annotation.targetName
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.compiletime.summonInline
import scala.concurrent.Promise
import scala.util.Try

class GraceException(
    val message: Option[String] = None,
    val cause: Option[Throwable] = None
) extends Exception(message.orNull, cause.orNull):
  def this(message: String, cause: Throwable) = this(Some(message), Some(cause))
  def this(message: String) = this(Some(message), None)
  def this(cause: Throwable) = this(None, Some(cause))

class terminal extends scala.annotation.StaticAnnotation

trait Execute[R[_], N[+_], C, A, B]:
  def apply(compiled: N[A], conn: C): B

object Execute:
  given execLifted[R[_], N[+_], C, A, B, G[_]](using
      execUnlifted: Execute[R, N, C, A, B],
      run: FromRunnable[G]
  ): Execute[R, N, C, A, G[B]] with
    def apply(compiled: N[A], conn: C): G[B] =
      run(() => execUnlifted(compiled, conn))

class Exe[R[_], N[+_], C, A](val compiled: N[A]):
  inline def apply[B](using conn: C): B =
    summonInline[Execute[R, N, C, A, B]].apply(compiled, conn)
  inline def as[G[_]](using C): G[A] =
    apply[G[A]]
  inline def run(using C): A = as[Id].unwrap
  inline def future(using C): Future[A] = as[Future]
  inline def promise(using C): Promise[A] = as[Promise]
  inline def asTry(using C): Try[A] = as[Try]
  inline def option(using C): Option[A] = as[Option]
  inline def either(using C): Either[Throwable, A] =
    as[[x] =>> Either[Throwable, x]]


sealed trait Raw    

trait Api:
  extension (bin: Raw)
    def typed[A]: A

  extension [A](a: A) def lift: Raw
  extension (sc: StringContext) def native(s: Raw*): Raw

  object function:

    inline def nullary[A](inline f: Raw): () => A = 
      () => f.typed[A]
    inline def unary[A, B](inline f: Raw => Raw): A => B = 
      a => f(a.lift).typed[B]
    inline def binary[A1, A2, B](inline f: (Raw, Raw) => Raw): (A1, A2) => B = 
      (a1, a2) => f(a1.lift, a2.lift).typed[B]
    inline def ternary[A1, A2, A3, B](inline f: (Raw, Raw, Raw) => Raw): (A1, A2, A3) => B =
      (a1, a2, a3) => f(a1.lift, a2.lift, a3.lift).typed[B]
    inline def quarternary[A1, A2, A3, A4, B](inline f: (Raw, Raw, Raw, Raw) => Raw): (A1, A2, A3, A4) => B =
      (a1, a2, a3, a4) => f(a1.lift, a2.lift, a3.lift, a4.lift).typed[B]

trait Queryable[R[_], M[_]]
    extends Relational[[x] =>> Source[R, M, x]]
    with Api:
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
    def insert(a: A): Unit

    inline def +=(a: A): Unit = insert(a)
    @terminal
    def update(predicate: A => Boolean)(f: A => A): Int
    @terminal
    def dropWhile(predicate: A => Boolean): Int
    @terminal
    def clear(): Int
    inline def truncate(): Int = clear()
    @terminal
    def create(): Unit
    @terminal
    def delete(): Unit

trait Context[R[_]]:
  self =>
  type Native[+A]
  type Api
  type Connection
  type Read[A]

  final type Execute[A, B] = graceql.core.Execute[R, Native, Connection, A, B]

  type Exe[A] <: graceql.core.Exe[R, Native, Connection, A]

  protected def exe[A](compiled: Native[A]): Exe[A]

  inline def compile[A](inline query: Api ?=> A): Native[Read[A]]

  inline def tryCompile[A](inline query: Api ?=> A): Try[Native[Read[A]]]

  inline def apply[A](inline query: Api ?=> A): Exe[Read[A]] =
    exe(compile(query))

  inline def tried[A](inline query: Api ?=> A): Try[Exe[Read[A]]] =
    tryCompile[A](query).map(exe)

final type Read[R[_], M[_], T] = T match
  case (k, grpd)       => (k, Read[R, M, grpd])
  case Source[R, M, a] => M[Read[R, M, a]]
  case _               => T

trait QueryContext[R[_], M[+_]] extends Context[R]:
  self =>

  final type Queryable = graceql.core.Queryable[R, M]
  final type Api = Queryable

  final type Read[T] = graceql.core.Read[R, M, T]
  final class Exe[A](compiled: Native[A])
      extends graceql.core.Exe[R, Native, Connection, A](compiled):
    type RowType = A match
      case M[a] => a
      case _    => A
    inline def transform[D[_]](using Connection)(using
        eq: A =:= M[RowType]
    ): D[RowType] =
      apply[D[RowType]]
    inline def lazyList(using Connection)(using
        A =:= M[RowType]
    ): LazyList[RowType] =
      transform[LazyList]
    inline def stream(using Connection)(using
        A =:= M[RowType]
    ): LazyList[RowType] =
      lazyList
  protected def exe[A](compiled: Native[A]): Exe[A] = Exe(compiled)

trait Acid[C]:
  def open(connection: C): C
  def commit(connection: C): Unit
  def rollback(connection: C): Unit

object Acid:
end Acid

object Transaction:
  extension [C, R](connection: C)
    def transaction[T[_]](using
        acid: Acid[C],
        run: FromRunnable[T],
        me: MonadError[T]
    ): ContT[R, T, C] =
      ContT.apply[R, T, C] { todo =>
        for
          session <- run(() => acid.open(connection))
          thunk = for
            r <- todo(session)
            _ <- run(() => acid.commit(session))
          yield r
          r <- thunk.recoverWith { e =>
            for
              _ <- run(() => acid.rollback(session))
              a <- me.raiseError[R](e)
            yield a
          }
        yield r
      }
