package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*

final type MySQL

object MySQL:
  given mysqlQueryContext[S[+X] <: Iterable[X]]: JDBCQueryContext[MySQL, S] with
    inline def compile[A](inline query: Capabilities ?=> A): Native[A] =
      ${ Compiler.compile[S,A]('query) }
