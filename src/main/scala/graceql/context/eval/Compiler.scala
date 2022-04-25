package graceql.context.eval

import scala.quoted.*
import graceql.compiler.Util
import graceql.core.SqlLike
import graceql.data.Source
import graceql.context.eval.*

object Compiler {
  def compile[A](e: Expr[A])(using q: Quotes, ta: Type[A]): Expr[() => A] = 
    '{() => $e}
}

