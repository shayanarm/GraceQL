package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection}
import java.sql.ResultSet

case class Table[V, +A](name: String)

enum DBIO[+A](val underlying: String, val parse: ResultSet => A):
  case Query[+A](val query: String, override val parse: ResultSet => A) extends DBIO[A](query, parse)
  case Update(val stmt: String) extends DBIO[Int](stmt, rs => {rs.next(); rs.getInt(0)})
  case Statement(val stmt: String) extends DBIO[Unit](stmt, _ => ())
  override def toString() = underlying
  
object DBIO:
  def query[A](query: String, parse: ResultSet => A): DBIO[A] = DBIO.Query(query, parse)
  def update(stmt: String): DBIO[Int] = DBIO.Update(stmt)
  def statement(stmt: String): DBIO[Unit] = DBIO.Statement(stmt)

trait JDBCQueryContext[V, S[+X] <: Iterable[X]] extends QueryContext[[x] =>> Table[V, x], S]: 

  type Native[+A] = DBIO[A]

  type Connection = JConnection

object Table:

  given execSync[V, A, L, T]: Execute[[x] =>> Table[V, x], DBIO, JConnection, A, A] with
    def apply(compiled: DBIO[A], conn: JConnection): A = ???

