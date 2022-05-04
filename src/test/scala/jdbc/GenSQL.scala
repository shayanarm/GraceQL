package jdbc

import graceql.core.*
import graceql.context.jdbc.*

final type GenSQL
object GenSQL:
  given anySQLContext[S[+X] <: Iterable[X]]: JDBCQueryContext[GenSQL, S] with
    inline def compile[A](inline query: Queryable ?=> A): Native[A] =
      ${ ParseOnlyCompiler.compileDML[S, A]('query) }
