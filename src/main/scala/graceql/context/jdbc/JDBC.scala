package graceql.context.jdbc

import graceql.core.*
import graceql.context.jdbc.Compiler

case class Table[V, +A](name: String)

type JDBCContext[V, S[_]] = Context[[x] =>> Table[V, x], S]

object Table {
  given jdbcContext[V, S[X] <: Iterable[X]]: Context[[x] =>> Table[V, x], S] with
    type Compiled[A] = String

    type Connection = DummyImplicit

    inline def compile[A](inline query: Queryable[[x] =>> Table[V, x], S] ?=> A): String = 
      ${ Compiler.compile[V, S, A]('query) }

  given execSync[V, A]: Execute[[x] =>> Table[V, x], [x] =>> String, DummyImplicit, A, A] with
    def apply(compiled: String, conn: DummyImplicit): A = ???
}

class Database

