package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type PostgreSQL
object PostgreSQL:
  given postgresQueryContext[S[+X] <: Iterable[X]]: JDBCQueryContext[PostgreSQL, S]
    with
    inline def compile[A](inline query: Capabilities ?=> A): Native[A] =
      ${ Compiler.compile[S,A]('query) }
    inline def tryCompile[A](inline query: Capabilities ?=> A): Try[Native[A]] =
      ${ Compiler.tryCompile[S,A]('query) }
