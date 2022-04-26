package graceql.context.jdbc

import scala.quoted.*
import graceql.util.CompileOps

object Compiler {
  import CompileOps.*
  def compile(e: Expr[Any])(using q: Quotes): Expr[Any] =
    import q.reflect.*
    val pipe =
        inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    logged(pipe)(e.asTerm).asExpr
}
