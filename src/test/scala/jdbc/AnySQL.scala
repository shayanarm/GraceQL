package jdbc

import graceql.core.*
import graceql.context.jdbc.*

final type AnySQL
object AnySQL:
  given anySQLContext[S[+X] <: Iterable[X]]: JDBCQueryContext[AnySQL, S] with
    inline def compile[A](inline query: Queryable ?=> A): Native[A] =
      ${ ParseOnlyCompiler.compileDML[S, A]('query) }
