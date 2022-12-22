package jdbc

import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.Compiled

final type GenSql
object GenSql:
  given anySqlContext[S[+X] <: Iterable[X]]: JdbcQueryContext[GenSql, S] with
    inline def compile[A](inline query: Capabilities ?=> A): Compiled[Native[A]] =
      ${ ParseOnlyCompiler.compile[S,A]('query) }