package graceql.core

import graceql.data.*
import scala.quoted.*
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

trait Execute[R[_], N[_], C, A, B]:
  def apply(compiled: N[A], conn: C): B

object Execute:
  given execLifted[R[_], N[_], C, A, B, G[_]](using
      execUnlifted: Execute[R, N, C, A, B],
      run: RunLifted[G]
  ): Execute[R, N, C, A, G[B]] with
    def apply(compiled: N[A], conn: C): G[B] =
      run(() => execUnlifted(compiled, conn))

class Exe[R[_], N[_], C, A](val compiled: N[A]):
  inline def apply[B](using conn: C): B =
    summonInline[Execute[R, N, C, A, B]].apply(compiled, conn)
  inline def as[G[_]](using C): G[A] =
    apply[G[A]]
  inline def run(using C): A = as[[x] =>> x]
  inline def future(using C): Future[A] = as[Future]
  inline def promise(using C): Promise[A] = as[Promise]
  inline def asTry(using C): Try[A] = as[Try]
  inline def option(using C): Option[A] = as[Option]
  inline def either(using C): Either[Throwable, A] =
    as[[x] =>> Either[Throwable, x]]

trait Capabilities[N[+_]]:
  extension [A](bin: N[A])
    def typed[B]: N[B]
    def unlift: A

  extension (sc: StringContext) def native(s: N[Any]*): N[Any]

  extension [A](a: A)
    def lift: N[A]

final type Read[R[_], M[_], T] = T match
  case (k, grpd)       => (k, Read[R, M, grpd])
  case Source[R, M, a] => M[Read[R, M, a]]
  case _               => T

trait Queryable[R[_], M[_], N[+_]]
    extends Relational[[x] =>> Source[R, M, x]]
    with Capabilities[N]:

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
    def dropWhile(predicate: A => Boolean): Int
    @terminal
    def clear(): Int
    @terminal
    inline def truncate(): Int = clear()
    @terminal
    def create(): Unit
    @terminal
    def delete(): Unit

trait Context[R[_]]:
  self =>
  type Native[+A]
  type Capabilities
  type Connection

  final type Execute[A, B] = graceql.core.Execute[R, Native, Connection, A, B]

  type Exe[A] <: graceql.core.Exe[R, Native, Connection, A]

  protected def exe[A](compiled: Native[A]): Exe[A]

  inline def apply[A](inline query: Capabilities ?=> A): Exe[A] =
    exe(compile(query))

  inline def tried[A](inline query: Capabilities ?=> A): Try[Exe[A]] =
    tryCompile[A](query).map(exe)

  inline def compile[A](inline query: Capabilities ?=> A): Native[A]
  
  inline def tryCompile[A](inline query: Capabilities ?=> A): Try[Native[A]]

trait QueryContext[R[_], M[+_]] extends Context[R]:
  self =>

  final type Queryable = graceql.core.Queryable[R, M, Native]
  final type Capabilities = Queryable
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
    @scala.annotation.nowarn
    final def apply[A](block: C ?=> T[A]): T[A] = tr match
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
    @scala.annotation.nowarn
    final def withFilter(pred: C => Boolean): Transaction[T, C, Nothing] =
      tr match
        case conn @ Continuation(_, _, _, _, me) =>
          pred(conn.session) match
            case true => tr
            case false =>
              throw new NoSuchElementException(
                "Transaction.withFilter predicate is not satisfied. Also, this method should not be called"
              )
    @scala.annotation.nowarn
    final def flatMap[C2, A](
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
