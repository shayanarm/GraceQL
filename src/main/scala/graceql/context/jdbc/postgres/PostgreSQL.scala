package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type PostgreSql
object PostgreSql:
  given postgresQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[PostgreSql, S]
    with
    inline def compile[A](inline query: Api ?=> A): Native[Read[A]] =
      ${ Compiler.compile[S, A]('query) }
    inline def tryCompile[A](inline query: Api ?=> A): Try[Native[Read[A]]] =
      ${ Compiler.tryCompile[S, A]('query) }
