package graceql.core

import graceql.data.*
import graceql.annotation.terminal
import scala.annotation.targetName
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.compiletime.summonInline
import scala.concurrent.Promise
import scala.util.Try

trait SqlLike[R[_], M[_]] extends Queryable[[x] =>> Source[R, M, x]]:

  type WriteResult

  final type Read[T] = T match
    case (k, grpd)       => (k, Read[grpd])
    case Source[R, M, a] => M[Read[a]]
    case _               => T

  extension [A](a: A)
    @terminal
    def read: Read[A]
  extension [A](values: M[A])
    @targetName("valuesAsSource")
    inline def asSource: Source[R, M, A] = Source.Values(values)
  extension [A](ref: R[A])
    @targetName("refAsSource")
    inline def asSource: Source[R, M, A] = Source.Ref(ref)
    @terminal
    def insertMany(a: Source[R, M, A]): WriteResult
    @terminal
    inline def ++=(a: Source[R, M, A]): WriteResult = insertMany(a)
    @terminal
    inline def insert(a: A): WriteResult = insertMany(a.pure)
    @terminal
    inline def +=(a: A): WriteResult = insert(a)
    @terminal
    def update(predicate: A => Boolean)(a: A => A): WriteResult
    @terminal
    def delete(a: A => Boolean): WriteResult
    @terminal
    inline def truncate(): WriteResult = delete(_ => true)

class GraceException(val message: String = null, val cause: Throwable = null)
    extends Exception(message, cause):
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(null, cause)

trait Execute[R[_], Compiled[_], Connection ,A , B]:
    def apply(compiled: Compiled[A], conn: Connection): B

object Execute:
  given execLifted[R[_], Compiled[_], Connection, A, B, G[_]](using
      execUnlifted: Execute[R, Compiled, Connection, A, B],
      run: RunLifted[G]
  ): Execute[R, Compiled, Connection, A, G[B]] with
    def apply(compiled: Compiled[A], conn: Connection): G[B] =
      run(() => execUnlifted(compiled, conn))  

trait Context[R[_], M[_]]:
  self =>

  type Compiled[A]

  type Connection

  final type Execute[A,B] = graceql.core.Execute[R, Compiled, Connection, A, B]

  inline def as[A, B](compiled: Compiled[A])(using conn: Connection): B =
    summonInline[Execute[A, B]].apply(compiled, conn)

  final class Exe[A](val compiled: Compiled[A]):
    type RowType = A match
      case M[a] => a
      case _    => A
    inline def as[C[_]](using Connection): C[A] =
      self.as[A, C[A]](compiled)
    inline def run(using Connection): A = as[[x] =>> x]
    inline def future(using Connection): Future[A] = as[Future]
    inline def promise(using Connection): Promise[A] = as[Promise]
    inline def asTry(using Connection): Try[A] = as[Try]
    inline def option(using Connection): Option[A] = as[Option]
    inline def either(using Connection): Either[Throwable, A] =
      as[[x] =>> Either[Throwable, x]]
    inline def transform[D[_]](using Connection)(using eq: A =:= M[RowType]): D[RowType] =
      self.as[M[RowType], D[RowType]](eq.liftCo[Compiled](compiled))
    inline def lazyList(using Connection)(using A =:= M[RowType]): LazyList[RowType] =
      transform[LazyList]
    inline def stream(using Connection)(using A =:= M[RowType]): LazyList[RowType] =
      lazyList

  inline def apply[A](inline query: SqlLike[R, M] ?=> A): Exe[A] =
    Exe(compile(query))

  inline def compile[A](inline query: SqlLike[R, M] ?=> A): Compiled[A]

trait MappedContext[R[_], M1[_], M2[_]](using val baseCtx: Context[R, M1])
    extends Context[R, M2]:
  def mapCapability(sl: SqlLike[R, M1]): SqlLike[R, M2]
  inline def mapCompiled[A](inline exe: baseCtx.Compiled[A]): Compiled[A]
  inline def compile[A](inline query: SqlLike[R, M2] ?=> A): Compiled[A] =
    mapCompiled(baseCtx.compile { sl ?=> query(using mapCapability(sl)) })

trait ACID[C]:
  def session(connection: C): C
  def open(connection: C): Unit
  def commit(connection: C): Unit
  def rollback(connection: C): Unit

object ACID:
  given ACID[DummyImplicit] with
    def session(connection: DummyImplicit): DummyImplicit = connection
    def open(connection: DummyImplicit): Unit = ()
    def commit(connection: DummyImplicit): Unit = ()
    def rollback(connection: DummyImplicit): Unit = ()

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
    final def withFilter(pred: C => Boolean): Transaction[T, C, Nothing] =
      tr match
        case conn @ Continuation(_, _, _, _, me) =>
          pred(conn.session) match
            case true => tr
            case false =>
              throw new NoSuchElementException(
                "Transaction.withFilter predicate is not satisfied. Also, this method should not be called"
              )
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
