package graceql.context.memory

import scala.quoted.*

object Compiler {
  def compile[A](e: Expr[A])(using q: Quotes, ta: Type[A]): Expr[() => A] = 
    '{() => $e}
}

