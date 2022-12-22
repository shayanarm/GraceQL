package graceql.quoted

import graceql.core.GraceException
import scala.quoted.*

enum Compiled[+A]:
  case Success(val code: A) extends Compiled[A]
  case Failure(val exc: GraceException) extends Compiled[Nothing]

object Compiled:
  import Compiled.*

  given toTry[A]: Conversion[Compiled[A], scala.util.Try[A]] = {
    case Success(c) => scala.util.Success(c)
    case Failure(v) => scala.util.Failure(v)
  }

  def get[A](expr: Expr[Compiled[A]])(using q: Quotes, ta: Type[A]): Expr[A] =
    import q.reflect.*
    val unlifted = CompileOps.inlineDefs(expr.asTerm).asExpr match
      case '{ Compiled.Success($code: A) }                => Right(code)
      case '{ new Compiled.Success($code: A) }            => Right(code)
      case '{ Compiled.Failure($ex: GraceException) }     => Left(ex)
      case '{ new Compiled.Failure($ex: GraceException) } => Left(ex)
      case _ => throw GraceException(expr.show)
    unlifted match
      case Right(code) => code
      case Left('{ GraceException(${ Expr(msg) }: String) }) => throw GraceException(msg)
      case Left('{ GraceException($c: Throwable) }) => throw GraceException("Compilation failed with a cause that could not be evaluated")
      case Left('{ GraceException(${ Expr(msg) }: String, $c: Throwable) }) => throw GraceException(msg)
      case _ => throw GraceException("Compilation could not be unlifted")

  def `catch`[A](thunk: => Expr[A])(using q: Quotes, ta: Type[A]): Expr[Compiled[A]] = 
    import q.reflect.*
    try   
      '{Compiled.Success($thunk)}
    catch
      case e =>
        '{Failure(GraceException(${Expr(e.getMessage)}))}
    

