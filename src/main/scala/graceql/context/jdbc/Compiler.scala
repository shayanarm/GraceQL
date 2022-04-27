package graceql.context.jdbc

import scala.quoted.*
import graceql.core.Queryable
import graceql.util.CompileOps

object Compiler {
  import CompileOps.*
  def compile[V, S[X] <: Iterable[X], A](e: Expr[Queryable[[x] =>> Table[V,x], S] ?=> A])(using q: Quotes): Expr[String] =
    import q.reflect.*
    val pipe =
        inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    logged(pipe)(e.asTerm).asExpr
    ???
}
