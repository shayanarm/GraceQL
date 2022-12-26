package jdbc

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try

final type GenSql
object GenSql:
  given anySqlContext[S[+X] <: Iterable[X]]: JdbcQueryContext[GenSql, S] with
    inline def compile[A](inline query: Api ?=> A): Native[Read[A]] =
      ${ ParseOnlyCompiler.compile[S, A]('query) }
    inline def tryCompile[A](inline query: Api ?=> A): Try[Native[Read[A]]] =
      ${ ParseOnlyCompiler.tryCompile[S, A]('query) }