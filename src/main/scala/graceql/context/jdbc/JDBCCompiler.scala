package graceql.context.jdbc

import scala.quoted.*
import graceql.core.*
import graceql.util.CompileOps
import scala.annotation.targetName

trait VendorTreeCompiler[V]:

  import CompileOps.*

  @targetName("compileScala")
  def compile[V, S[+X] <: Iterable[X], A](
      e: Expr[Queryable[[x] =>> Table[V, x], S, [x] =>> Tree] ?=> A]
  )(using q: Quotes): Expr[Tree] =
    import q.reflect.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    logged(pipe)(e.asTerm).asExpr
    ???
  @targetName("compileNative")
  def compile[S[+X] <: Iterable[X], A](e: Expr[Tree])(using
      q: Quotes
  ): Expr[Tree] = ???
