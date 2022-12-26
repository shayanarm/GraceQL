package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try


final type MySql

object MySql:
  given mysqlQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[MySql, S] with
    inline def compile[A](inline query: Api ?=> A): Native[Read[A]] =
      ${ Compiler.compile[S, A]('query) }
    inline def tryCompile[A](inline query: Api ?=> A): Try[Native[Read[A]]] =
      ${ Compiler.tryCompile[S, A]('query) }