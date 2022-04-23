package graceql.core

import scala.quoted.*
import graceql.data.*
import graceql.annotation.terminal
import scala.annotation.targetName
import scala.reflect.TypeTest
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.compiletime.summonInline
import scala.concurrent.Promise

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

  trait Run[A, B]:
    def apply(compiled: Compiled[A], conn: Connection): B
    def lift(a: A): B
  
  object Run:
    given runFuture[A](using run: Run[A, A], ec: ExecutionContext): Run[A, Future[A]] with
      def apply(compiled: Compiled[A], conn: Connection): Future[A] = 
        Future{run(compiled,conn)}
      def lift(a: A) = Future.successful(a)  

    given runPromise[A](using run: Run[A, Future[A]]): Run[A, Promise[A]] with
      def apply(compiled: Compiled[A], conn: Connection): Promise[A] = 
        Promise[A].completeWith(run(compiled,conn))
      def lift(a: A) = Promise.successful(a)
    
    given runNested[A, FA, G[_]](using runf: Run[A, FA], rung: Run[FA,G[FA]]): Run[A, G[FA]] with
      def apply(compiled: Compiled[A], conn: Connection): G[FA] = 
        rung.lift(runf(compiled,conn))
      def lift(a: A) = rung.lift(runf.lift(a))

  inline def run[A, B](compiled: Compiled[A])(using conn: Connection): B =
    summonInline[Run[A,B]].apply(compiled, conn)
 
  final class Exe[A](val compiled: Compiled[A]):
    type RowType = A match
      case M[a] => a
      case _ => A
    inline def runAs[C[_]]()(using conn: Connection): C[A] = self.run[A,C[A]](compiled)
    inline def run()(using conn: Connection): A = runAs[[x] =>> x]()
    inline def future()(using conn: Connection): Future[A] = runAs[Future]()
    inline def promise()(using conn: Connection): Promise[A] = runAs[Promise]()
    inline def transform[D[_]]()(using eq: A =:= M[RowType], conn: Connection): D[RowType] = 
      self.run[M[RowType], D[RowType]](eq.liftCo[Compiled](compiled))
    inline def lazyList()(using eq: A =:= M[RowType], conn: Connection): LazyList[RowType] = 
      transform[LazyList]()      
    inline def stream()(using eq: A =:= M[RowType], conn: Connection): LazyList[RowType] =
      lazyList()
                 
  inline def apply[A](inline query: SqlLike[R, M] ?=> A): Exe[A] =
    Exe(compile(query))

  inline def compile[A](inline query: SqlLike[R, M] ?=> A): Compiled[A]


trait MappedContext[R[_], M2[_], M1[_]](using val baseCtx: Context[R,M1]) extends Context[R, M2]:
  def mapCapability(sl: SqlLike[R, M1]): SqlLike[R,M2]
  inline def mapCompiled[A](inline exe: baseCtx.Compiled[A]): Compiled[A]
  inline def compile[A](inline query: SqlLike[R, M2] ?=> A): Compiled[A] =
    mapCompiled(baseCtx.compile {sl ?=> query(using mapCapability(sl)) }) 