package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.Tried

final type PostgreSql
object PostgreSql:
  given postgresQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[PostgreSql, S]
    with
    inline def compile[A](inline query: Api ?=> A): Tried[Native[A]] =
      ${ Compiler.compile[S,A]('query) }
