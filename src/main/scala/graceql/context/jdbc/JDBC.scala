package graceql.context.jdbc

import graceql.core.*
import graceql.context.jdbc.JDBCCompiler
import java.sql.{Connection => JConnection}

case class Table[V, +A](name: String)

trait VendorContext[V, S[+X] <: Iterable[X]]:
  inline def compile[A](inline query: Queryable[[x] =>> Table[V, x], S, [x] =>> String] ?=> A): String

object Table:
  given jdbcContext[V, S[+X] <: Iterable[X]](using vc: VendorContext[V,S]): QueryContext[[x] =>> Table[V, x], S] with
    type Binary[A] = String

    type Connection = JConnection

    inline def compile[A](inline query: Queryable ?=> A): String = 
      vc.compile(query)

  given execSync[V, A]: Execute[[x] =>> Table[V, x], [x] =>> String, JConnection, A, A] with
    def apply(compiled: String, conn: JConnection): A = ???

class Database

