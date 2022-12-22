package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.Compiled

final type PostgreSql
object PostgreSql:
  given postgresQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[PostgreSql, S]
    with
    inline def compile[A](inline query: Api ?=> A): Compiled[Native[A]] =
      ${ Compiler.compile[S,A]('query) }
