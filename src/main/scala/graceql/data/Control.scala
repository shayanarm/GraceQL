package graceql.data

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.util.Try
import java.{util => ju}
import graceql.syntax.*

trait Functor[M[_]]:
  extension [A](ma: M[A]) 
    def map[B](f: A => B): M[B]
    inline def ^^[B](f: A => B): M[B] = map(f)

trait Applicative[M[_]] extends Functor[M]:
  extension [A](a: A) @`inline` def pure: M[A]
  extension [A](ma: M[A])

    def ap[B](mf: M[A => B]): M[B]
    
    def zip[B](mb: M[B]): M[(A, B)] = ma.ap(mb.map(b => a => (a, b)))
    
    inline def ~[B](mb: M[B]): M[(A, B)] = zip(mb)
    
    inline def <*>[B](f: M[A => B]): M[B] = ap(f)
    def map[B](f: A => B): M[B] =
      ma <*> f.pure

  inline def pass: M[Unit] = ().pure

object Applicative

trait Monad[M[_]] extends Applicative[M]:
  extension [A](ma: M[A])

    def flatMap[B](f: A => M[B]): M[B]

    inline def >>=[B](f: A => M[B]): M[B] = flatMap(f)

    inline def bind[B](f: A => M[B]): M[B] = flatMap(f)
    def ap[B](mf: M[A => B]): M[B] = flatMap(a => mf.flatMap(f => f(a).pure))

object Monad

trait MonadPlus[M[_]] extends Monad[M]:
  extension [A](ma: M[A])

    def concat(other: M[A]): M[A]

    inline def union(other: M[A]): M[A] = concat(other)

    inline def ++(other: M[A]): M[A] = concat(other)

object MonadPlus

trait MonadZero[M[_]] extends Monad[M]:
  extension [A](ma: M[A])

    def filter(pred: A => Boolean): M[A]

    def withFilter(pred: A => Boolean): scala.collection.WithFilter[A, M]

object MonadZero

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
    def raiseError[A](e: Throwable): Try[A] = Try { throw e }
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
        Try { ma.get }.fold(f, Some(_))

  given eitherME: MonadError[[x] =>> Either[Throwable, x]] with
    def raiseError[A](e: Throwable): Either[Throwable, A] = Left(e)
    extension [A](a: A) @`inline` def pure: Either[Throwable, A] = Right(a)
    extension [A](ma: Either[Throwable, A])
      def flatMap[B](f: A => Either[Throwable, B]): Either[Throwable, B] =
        ma.flatMap(f)
      def recoverWith(
          f: Throwable => Either[Throwable, A]
      ): Either[Throwable, A] =
        ma.left.flatMap[Throwable, A](f)

trait Traverse[F[_]]:
  def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: F[A]): G[F[B]]

  def sequence[G[_]: Applicative, A](fa: F[G[A]]): G[F[A]] = traverse[G, G[A], A](identity)(fa)

object Traverse:

  given Traverse[List] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: List[A]): G[List[B]] =
        fa.foldRight(List.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => a :: b)
        }

  given Traverse[Vector] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: Vector[A]): G[Vector[B]] =
        fa.foldRight(Vector.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => a +: b)
        }
        
  given Traverse[IndexedSeq] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: IndexedSeq[A]): G[IndexedSeq[B]] =
        fa.foldRight(IndexedSeq.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => a +: b)
        }

  given Traverse[LazyList] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: LazyList[A]): G[LazyList[B]] =
        fa.foldRight(LazyList.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => a +: b)
        }

  given Traverse[Option] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: Option[A]): G[Option[B]] =
        fa.fold(Option.empty[B].pure[G]) {v => f(v).map(Some(_))}
  
  given Traverse[Seq] with 
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: Seq[A]): G[Seq[B]] =
        fa.foldRight(Seq.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => a +: b)
        }  
  given Traverse[Iterable] with
    override def traverse[G[_]: Applicative, A, B](f: A => G[B])(fa: Iterable[A]): G[Iterable[B]] =
        fa.foldRight(Iterable.empty[B].pure[G]) { (a, acc) =>
          f(a).zip(acc).map((a,b) => List(a) ++ b)
        }        

trait FromRunnable[M[_]]:
  def apply[A](a: () => A): M[A]

object FromRunnable:

  given fromId: FromRunnable[Id] with

    def apply[A](a: () => A): Id[A] = Id(a())

  given fromFuture[A](using
      ec: ExecutionContext
  ): FromRunnable[Future] with
    def apply[A](a: () => A): Future[A] =
      Future { a() }

  given fromPromise[A](using run: FromRunnable[Future]): FromRunnable[Promise] with
    def apply[A](a: () => A): Promise[A] =
      Promise[A].completeWith(run(a))

  given fromTry[A]: FromRunnable[Try] with
    def apply[A](a: () => A): Try[A] =
      Try { a() }

  given fromOption[A]: FromRunnable[Option] with
    def apply[A](a: () => A): Option[A] =
      summon[FromRunnable[Try]](a).toOption

  given fromEither[A]: FromRunnable[[x] =>> Either[Throwable, x]] with
    def apply[A](a: () => A): Either[Throwable, A] =
      summon[FromRunnable[Try]](a).toEither
