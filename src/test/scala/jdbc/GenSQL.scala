package jdbc

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type GenSql
object GenSql:
  given anySqlContext[S[+X] <: Iterable[X]]: JdbcQueryContext[GenSql, S] with
    inline def compile[A](inline query: Queryable ?=> A): Native[A] =
      ${ ParseOnlyCompiler.compile[S, A]('query) }
    inline def tryCompile[A](inline query: Capabilities ?=> A): Try[Native[A]] =
      ${ ParseOnlyCompiler.tryCompile[S,A]('query) }