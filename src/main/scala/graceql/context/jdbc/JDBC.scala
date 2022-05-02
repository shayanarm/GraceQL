package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection}

case class Table[V, +A](name: String)

trait JDBCContext[V, S[+X] <: Iterable[X]] extends QueryContext[[x] =>> Table[V, x], S]:

  final type Tree = graceql.context.jdbc.Tree 

  type Native[+A] = Tree

  type Connection = JConnection


object Table:

  given execSync[V, A, L, T]: Execute[[x] =>> Table[V, x], [x] =>> Tree, JConnection, A, A] with
    def apply(compiled: Tree, conn: JConnection): A = ???

