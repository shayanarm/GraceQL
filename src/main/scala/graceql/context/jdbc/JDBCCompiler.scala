package graceql.context.jdbc

import scala.quoted.*
import graceql.core.Queryable
import graceql.util.CompileOps

abstract class JDBCCompiler[V]:
  import CompileOps.*
  def compile[S[+X] <: Iterable[X], A](e: Expr[Queryable[[x] =>> Table[V,x], S, [x] =>> String] ?=> A])(using q: Quotes): Expr[String] =
    import q.reflect.*
    val pipe =
        inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    logged(pipe)(e.asTerm).asExpr
    ???
