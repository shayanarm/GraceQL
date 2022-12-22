package graceql.quoted

import graceql.core.GraceException
import scala.quoted.*

enum Tried[+A]:
  case Success(val code: A) extends Tried[A]
  case Failure(val exc: GraceException) extends Tried[Nothing]

object Tried:
  import Tried.*

  given toTry[A]: Conversion[Tried[A], scala.util.Try[A]] = {
    case Success(c) => scala.util.Success(c)
    case Failure(v) => scala.util.Failure(v)
  }

  def get[A](expr: Expr[Tried[A]])(using q: Quotes, ta: Type[A]): Expr[A] =
    import q.reflect.*
    val unlifted = CompileOps.inlineDefs(expr.asTerm).asExpr match
      case '{ Tried.Success($code: A) }                => Right(code)
      case '{ new Tried.Success($code: A) }            => Right(code)
      case '{ Tried.Failure($ex: GraceException) }     => Left(ex)
      case '{ new Tried.Failure($ex: GraceException) } => Left(ex)
      case _ => throw GraceException(
        s"Expressions of type `Compiled[${Type.show[A]}]` must be constructed using one of its case constructors directly. See `Tried.catch` as an example."
        )
    unlifted match
      case Right(code) => code
      case Left('{ GraceException(${ Expr(msg) }: String) }) => throw GraceException(msg)
      case Left('{ GraceException($c: Throwable) }) => throw GraceException("Compilation failed with a cause that could not be evaluated")
      case Left('{ GraceException(${ Expr(msg) }: String, $c: Throwable) }) => throw GraceException(msg)
      case _ => throw GraceException("Compilation could not be unlifted")

  def `catch`[A](thunk: => Expr[A])(using q: Quotes, ta: Type[A]): Expr[Tried[A]] = 
    import q.reflect.*
    try   
      '{Tried.Success($thunk)}
    catch
      case e =>
        '{Failure(GraceException(${Expr(e.getMessage)}))}
    

