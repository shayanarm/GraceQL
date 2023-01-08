package graceql.data

import scala.collection.WithFilter
import java.util.NoSuchElementException
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import graceql.syntax.*

enum Validated[E, +A]:
  case Valid[E, A](value: A) extends Validated[E, A]
  case Invalid[E, A](head: E, tail: Seq[E] = Seq.empty) extends Validated[E, A]

  def toOption: Option[A] = this match
    case Valid(v) => Some(v)
    case _        => None

  def fold[B](ifInvalid: => B)(f: A => B): B = this match
    case Valid(v) => f(v)
    case _        => ifInvalid

  def getOrElse[B >: A](a: => B): B = this match
    case Valid(v) => v
    case _        => a

  def get(): A = getOrElse(throw NoSuchElementException())

  def errors: Seq[E] = this match
    case Valid(v)       => Seq.empty
    case Invalid(h, es) => h +: es

  def mapError[O](f: E => O): Validated[O, A] =
    this match
      case Invalid(h, t) => Invalid(f(h), t.map(f))
      case other         => other.asInstanceOf[Validated[O, A]]

  def filter(err: A => E)(pred: A => Boolean): Validated[E, A] =
    this match
      case v @ Valid(a) if !pred(a) => Invalid(err(a))
      case other                    => other

  inline def filter(err: => E)(pred: A => Boolean): Validated[E, A] =
    filter(_ => err)(pred)

  inline def filter(
      pred: A => Boolean
  )(using conv: Validated.FromString[E]): Validated[E, A] =
    filter(a =>
      conv(s"Predicate for `Validated.filter` failed for value `${a}`")
    )(pred)

  inline def withFilter(pred: A => Boolean)(using
      conv: Validated.FromString[E]
  ): Validated[E, A] =
    filter(pred)

object Validated:
  import Validated.*;

  trait FromString[+E]:
    def apply(msg: String): E

  object FromString:
    given FromString[String] = a => a
    given convertible[A](using c: Conversion[String, A]): FromString[A] = a =>
      c(a)

  def fromTry[A](t: Try[A]): Validated[Throwable, A] = t match
    case Failure(exception) => Invalid(exception, Seq.empty)
    case Success(value)     => Valid(value)

  inline def successfulEval[A](run: => A): Validated[Throwable, A] = fromTry(
    Try(run)
  )

  extension [E](e: E) def err[A]: Validated[E, A] = Invalid(e)

  extension [E](es: Seq[E])
    def asErrors[A](ifEmpty: A): Validated[E, A] = es match
      case h +: t => Invalid(h, t)
      case _      => Valid(ifEmpty)

  extension [A](a: Option[A])
    def toValid[E](err: => E): Validated[E, A] =
      a.fold(Invalid(err))(Valid.apply)

  given monad[E]: Monad[[x] =>> Validated[E, x]] with {

    extension [A](a: A) override def pure: Validated[E, A] = Valid(a)

    extension [A](ma: Validated[E, A])

      override def map[B](f: A => B): Validated[E, B] =
        ma match
          case Valid(a) => Valid(f(a))
          case other    => other.asInstanceOf[Validated[E, B]]

      override def ap[B](mf: Validated[E, A => B]): Validated[E, B] =
        (ma, mf) match
          case (Invalid(h, t), invr: Invalid[_, _]) =>
            Invalid(h, t ++ invr.errors)
          case (Valid(_), inv: Invalid[_, _]) =>
            inv.asInstanceOf[Validated[E, B]]
          case (inv: Invalid[_, _], Valid(_)) =>
            inv.asInstanceOf[Validated[E, B]]
          case (Valid(v), Valid(f)) => Valid(f(v))

    extension [A](ma: Validated[E, A])
      override def flatMap[B](f: A => Validated[E, B]): Validated[E, B] =
        ma match
          case Valid(a) => f(a)
          case other    => other.asInstanceOf[Validated[E, B]]
  }

class ValidatedT[M[_], E, A](val underlying: M[Validated[E, A]])
object ValidatedT:
  given monad[E, M[_]](using m: Monad[M]): Monad[[x] =>> ValidatedT[M, E, x]]
    with {

    extension [A](a: A)
      override def pure: ValidatedT[M, E, A] =
        ValidatedT(m.pure(summon[Monad[[x] =>> Validated[E, x]]].pure(a)))

    extension [A](ma: ValidatedT[M, E, A])

      override def map[B](f: A => B): ValidatedT[M, E, B] =
        ValidatedT(ma.underlying.map(va => va.map(f)))

      override def ap[B](mf: ValidatedT[M, E, A => B]): ValidatedT[M, E, B] =
        ValidatedT {
          for
            a <- ma.underlying
            f <- mf.underlying
          yield a.ap(f)
        }

    extension [A](ma: ValidatedT[M, E, A])
      override def flatMap[B](
          f: A => ValidatedT[M, E, B]
      ): ValidatedT[M, E, B] =
        ValidatedT {
          for
            va <- ma.underlying
            r <- va match
              case Validated.Valid(a) => f(a).underlying
              case inv @ Validated.Invalid(_, _) =>
                m.pure(inv.asInstanceOf[Validated[E, B]])
          yield r
        }
  }