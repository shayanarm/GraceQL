package graceql.context.troll

import scala.quoted.*
import graceql.compiler.Util

object Compiler {
  import Util.*
  def compile(e: Expr[Any])(using q: Quotes): Expr[Any] =
    import q.reflect.*
    val pipe =
        inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    logged(pipe)(e.asTerm).asExpr
}
