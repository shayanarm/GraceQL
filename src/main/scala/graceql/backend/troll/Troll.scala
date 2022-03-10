package graceql.backend.troll

import scala.quoted.*
import graceql.core.*
import graceql.backend.troll.Compiler

type troll

object troll {
  // transparent inline def apply(inline q: Any): Any = ${Compiler.compile('q)}
  // given Compile[troll] with
  //   transparent inline def apply(inline q: Any): Any = troll.apply(q)
}
