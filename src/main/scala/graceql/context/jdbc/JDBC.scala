package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection, Statement => _}
import java.sql.ResultSet

case class Table[V, +A](name: String)

class Statement[+A](val tree: Tree, val parse: ResultSet => A):
  override def toString() = tree.printer.compact
object Statement:
  given NativeSupport[Statement] with {}  

trait JDBCQueryContext[V, S[+X] <: Iterable[X]] extends QueryContext[[x] =>> Table[V, x], S]: 

  type Native[+A] = Statement[A]

  type Connection = JConnection

trait JDBCSchemaContext[V] extends SchemaContext[[x] =>> Table[V, x]]: 

  type Native[+A] = Statement[A]

  type Connection = JConnection


object Table:

  given execSync[V, A, L, T]: Execute[[x] =>> Table[V, x], Statement, JConnection, A, A] with
    def apply(compiled: Statement[A], conn: JConnection): A = ???

