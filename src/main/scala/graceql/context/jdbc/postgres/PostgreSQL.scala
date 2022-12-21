package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type PostgreSql
object PostgreSql:
  given postgresQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[PostgreSql, S]
    with
    inline def compile[A](inline query: Capabilities ?=> A): Native[A] =
      ${ Compiler.compile[S,A]('query) }
    inline def tryCompile[A](inline query: Capabilities ?=> A): Try[Native[A]] =
      ${ Compiler.tryCompile[S,A]('query) }
