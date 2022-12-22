package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import scala.util.Try
import graceql.quoted.Compiled

final type MySql

object MySql:
  given mysqlQueryContext[S[+X] <: Iterable[X]]: JdbcQueryContext[MySql, S] with
    inline def compile[A](inline query: Api ?=> A): Compiled[Native[A]] =
      ${ Compiler.compile[S,A]('query) }