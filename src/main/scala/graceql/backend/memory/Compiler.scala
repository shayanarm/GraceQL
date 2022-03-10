package graceql.backend.memory

import scala.quoted.*
import graceql.compiler.Util

object Compiler {
  def compile[A](e: Expr[A])(using q: Quotes, ta: Type[A]): Expr[A] = 
    import quotes.reflect.*
    println(e.asTerm.show(using Printer.TreeAnsiCode))
    e  
  // import Util.*
  // def compile[A](e: Expr[A])(using q: Quotes, ta: Type[A]): Expr[A] = 
  //   import q.reflect.*
  //   val pipe =
  //       inlineDefs andThen
  //       betaReduceAll andThen
  //       inlineDefs
  //   logged(pipe)(e.asTerm).asExprOf[A]
}

