package graceql.data

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Try
import java.{util => ju}

trait Functor[M[_]]:
  extension [A](ma: M[A]) def map[B](f: A => B): M[B]

trait Applicative[M[_]] extends Functor[M]:
  extension [A](a: A) @`inline` def pure: M[A]
  extension [A](ma: M[A])

    def ap[B](mf: M[A => B]): M[B]

    inline def <*>[B](f: M[A => B]): M[B] = ap(f)
    def map[B](f: A => B): M[B] =
      ma <*> f.pure

trait Monad[M[_]] extends Applicative[M]:
  extension [A](ma: M[A])

    def flatMap[B](f: A => M[B]): M[B]

    inline def >>=[B](f: A => M[B]): M[B] = flatMap(f)

    inline def bind[B](f: A => M[B]): M[B] = flatMap(f)
    def ap[B](mf: M[A => B]): M[B] = flatMap(a => mf.flatMap(f => f(a).pure))

trait MonadPlus[M[_]] extends Monad[M]:
  extension [A](ma: M[A])

    def concat(other: M[A]): M[A]

    inline def union(other: M[A]): M[A] = concat(other)

    inline def ++(other: M[A]): M[A] = concat(other)

trait MonadZero[M[_]] extends Monad[M]:
  extension [A](ma: M[A])

    def filter(pred: A => Boolean): M[A]

    def withFilter(pred: A => Boolean): scala.collection.WithFilter[A, M]

trait MonadError[M[_]] extends Monad[M]:
  def raiseError[A](e: Throwable): M[A]
  extension [A](ma: M[A]) def recoverWith(f: Throwable => M[A]): M[A]

object MonadError:
  given futureME(using ec: ExecutionContext): MonadError[Future] with
    def raiseError[A](e: Throwable): Future[A] = Future.failed(e)
    extension [A](a: A) @`inline` def pure: Future[A] = Future.successful(a)
    extension [A](ma: Future[A])
      def flatMap[B](f: A => Future[B]): Future[B] = ma.flatMap(f)
      def recoverWith(f: Throwable => Future[A]): Future[A] = ma.recoverWith {
        case t => f(t)
      }
  given tryME: MonadError[Try] with
    def raiseError[A](e: Throwable): Try[A] = Try {throw e }
    extension [A](a: A) @`inline` def pure: Try[A] = Try(a)
    extension [A](ma: Try[A])
      def flatMap[B](f: A => Try[B]): Try[B] = ma.flatMap(f)
      def recoverWith(f: Throwable => Try[A]): Try[A] = ma.recoverWith {
        case t => f(t)
      }

  given optionME: MonadError[Option] with
    def raiseError[A](e: Throwable): Option[A] = None
    extension [A](a: A) @`inline` def pure: Option[A] = Some(a)
    extension [A](ma: Option[A])
      def flatMap[B](f: A => Option[B]): Option[B] = ma.flatMap(f)
      def recoverWith(f: Throwable => Option[A]): Option[A] = 
        Try{ma.get}.fold(f,Some(_))      

  given eitherME: MonadError[[x] =>> Either[Throwable, x]] with
      def raiseError[A](e: Throwable): Either[Throwable, A] = Left(e)
      extension [A](a: A) @`inline` def pure: Either[Throwable, A] = Right(a)
      extension [A](ma: Either[Throwable, A])
        def flatMap[B](f: A => Either[Throwable, B]): Either[Throwable, B] = ma.flatMap(f)
        def recoverWith(f: Throwable => Either[Throwable, A]): Either[Throwable, A] = 
          ma.left.flatMap[Throwable,A](f)

trait RunLifted[M[_]]:
  def apply[A](a: () => A): M[A]

object RunLifted:

  given identityRun: RunLifted[[x] =>> x] with
    
    def apply[A](a: () => A): A = a()

  given runFuture[A](using
      ec: ExecutionContext
  ): RunLifted[Future] with
    def apply[A](a: () => A): Future[A] =
      Future { a() }

  given runPromise[A](using run: RunLifted[Future]): RunLifted[Promise] with
    def apply[A](a: () => A): Promise[A] =
      Promise[A].completeWith(run(a))

  given runTry[A]: RunLifted[Try] with
    def apply[A](a: () => A): Try[A] =
      Try { a() }

  given runOption[A](using
      run: RunLifted[Try]
  ): RunLifted[Option] with
    def apply[A](a: () => A): Option[A] =
      run(a).toOption

  given runEither[A](using
      run: RunLifted[Try]
  ): RunLifted[[x] =>> Either[Throwable, x]] with
    def apply[A](a: () => A): Either[Throwable, A] =
      run(a).toEither