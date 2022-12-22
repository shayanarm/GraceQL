package jdbc

import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.Tried

final type GenSql
object GenSql:
  given anySqlContext[S[+X] <: Iterable[X]]: JdbcQueryContext[GenSql, S] with
    inline def compile[A](inline query: Api ?=> A): Tried[Native[A]] =
      ${ ParseOnlyCompiler.compile[S,A]('query) }