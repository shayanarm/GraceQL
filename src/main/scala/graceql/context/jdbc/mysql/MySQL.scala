package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*

final type MySQL

object MySQL:
  given mysqlQueryContext[S[+X] <: Iterable[X]]: JDBCQueryContext[MySQL, S] with
    inline def compile[A](inline query: Capabilities ?=> A): Native[A] =
      ${ Compiler.compileDML[S,A]('query) }
  given mysqlSchemaContext: JDBCSchemaContext[MySQL] with
    inline def compile[A](inline query: Capabilities ?=> A): Native[A] =
      ${ Compiler.compileDDL[A]('query) }
