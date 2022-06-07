package jdbc

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type GenSQL
object GenSQL:
  given anySQLContext[S[+X] <: Iterable[X]]: JDBCQueryContext[GenSQL, S] with
    inline def compile[A](inline query: Queryable ?=> A): Native[A] =
      ${ ParseOnlyCompiler.compile[S, A]('query) }
    inline def tryCompile[A](inline query: Capabilities ?=> A): Try[Native[A]] =
      ${ ParseOnlyCompiler.tryCompile[S,A]('query) }