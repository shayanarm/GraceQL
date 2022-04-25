package graceql.context.troll

import scala.quoted.*
import graceql.core.*
import graceql.context.troll.Compiler

type troll

object troll {
  // transparent inline def apply(inline q: Any): Any = ${Compiler.compile('q)}
  // given Compile[troll] with
  //   transparent inline def apply(inline q: Any): Any = troll.apply(q)
}
