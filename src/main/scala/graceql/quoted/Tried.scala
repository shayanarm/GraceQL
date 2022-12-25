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
    import CompileOps.*

    val unlifted = inlineDefs(expr.asTerm).asExprOf[Tried[A]] match
        case '{ Success($code: A) }                => Right(code)
        case '{ new Success($code: A) }            => Right(code)
        case '{ Failure($ex: GraceException) }     => Left(ex)
        case '{ new Failure($ex: GraceException) } => Left(ex)
        case other => throw GraceException(
          s"""
          |Expressions of type `Compiled[${Type.show[A]}]` must be constructed using one of its case constructors directly. See `graceql.quoted.Tried.apply` as an example.
          |
          |Tree code:
          |
          |${other.asTerm.show(using Printer.TreeShortCode)}
          |
          |Tree structure:
          |
          |${other.asTerm.show(using Printer.TreeStructure)}
          """.stripMargin
          )
    unlifted match
      case Right(code) => code
      case Left('{ GraceException(${ Expr(msg) }: String) }) => throw GraceException(msg)
      case Left('{ GraceException($c: Throwable) }) => throw GraceException("Compilation failed without a message. This is heavily discouraged")
      case Left('{ GraceException(${ Expr(msg) }: String, $c: Throwable) }) => throw GraceException(msg)
      case _ => throw GraceException("Compilation could not be unlifted")

  def apply[A](thunk: => Expr[A])(using q: Quotes, ta: Type[A]): Expr[Tried[A]] = 
    import q.reflect.*
    try   
      '{Success($thunk)}
    catch
      case e =>
        '{Failure(GraceException(${Expr(e.getMessage)}))}
    

// val q = {
//   val memoryQueryContext_this: Context_this.type = Context_this

//   Success.apply((() => memoryQueryContext_this.inline$read({
//     val evidence$2$proxy1 = memoryQueryContext_this.inline$sl
//     evidence$2$proxy1.map({
//       val values$proxy1 = Seq.apply[Int](1, 2, 3)

//       Source.Values.apply(values$proxy1)
//     })(((_$1: Int) => _$1.+(2)))
//   })))
// }        

// val s = Success.apply((() => Context_this.inline$read(evidence$2$proxy1.map(Source.Values.apply(Seq.apply(1, 2, 3)))(((_$1: Int) => _$1.+(2))))))
