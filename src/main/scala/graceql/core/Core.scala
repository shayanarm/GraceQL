package graceql.core

import scala.quoted.*
import graceql.data.*
import graceql.annotation.terminal
import scala.annotation.targetName
import scala.reflect.TypeTest
import scala.concurrent.Future
import scala.compiletime.summonInline

trait SqlLike[R[_], M[_]] extends Queryable[[x] =>> Source[R, M, x]]:

  type WriteResult

  final type Read[T] = T match
    case (k, grpd) => (k, Read[grpd])
    case Source[R, M, a] => M[Read[a]]
    case _ => T
    
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


trait Context[R[_], M[_]]:
  self =>

  type Compiled[A]

  type Connection

  trait Run[C[_], A]:
    def apply(compiled: Compiled[A], conn: Connection): C[A]

  trait RunTransform[D[_], A]:
    def apply(compiled: Compiled[M[A]], conn: Connection): D[A]

  object RunTransform:
    given identityTransform[A](using run: Run[[x] =>> x, M[A]]): RunTransform[M,A] with
      def apply(compiled: Compiled[M[A]], conn: Connection): M[A] = 
        run(compiled, conn)

  inline def run[C[_], A](compiled: Compiled[A]): C[A] =
    summonInline[Run[C, A]].apply(compiled, summonInline[Connection])

  inline def runTransform[D[_], A](compiled: Compiled[M[A]]): D[A] =
    summonInline[RunTransform[D, A]].apply(compiled, summonInline[Connection])
 
  final class Exe[A](val compiled: Compiled[A]):
    type RowType = A match
      case M[a] => a
      case _ => A
    inline def runAs[C[_]]: C[A] = self.run[C, A](compiled)
    inline def run(): A = runAs[[x] =>> x]
    inline def future(): Future[A] = runAs[Future]
    inline def transform[D[_]](using eq: A =:= M[RowType]): D[RowType] = 
      self.runTransform[D, RowType](eq.liftCo[Compiled](compiled))
                 
  inline def apply[A](inline query: SqlLike[R, M] ?=> A): Exe[A] =
    Exe(compile(query))

  inline def compile[A](inline query: SqlLike[R, M] ?=> A): Compiled[A]


trait MappedContext[R[_], M2[_], M1[_]](using val baseCtx: Context[R,M1]) extends Context[R, M2]:
  def mapCapability(sl: SqlLike[R, M1]): SqlLike[R,M2]
  inline def mapCompiled[A](inline exe: baseCtx.Compiled[A]): Compiled[A]
  inline def compile[A](inline query: SqlLike[R, M2] ?=> A): Compiled[A] =
    mapCompiled(baseCtx.compile {sl ?=> query(using mapCapability(sl)) })