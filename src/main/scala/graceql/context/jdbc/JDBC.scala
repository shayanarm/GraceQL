package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection, ResultSet}

final case class Table[V, +A]()

object Table:
  given execSync[V, A, L, T]
      : Execute[[x] =>> Table[V, x], DBIO, JConnection, A, A] with
    def apply(compiled: DBIO[A], conn: JConnection): A =
      try
        val stmt = conn.createStatement()
        compiled match
          case DBIO.Query(q, p) =>
            p(stmt.executeQuery(q))
          case DBIO.Update(s) =>
            stmt.executeUpdate(s)
          case DBIO.Statement(s) =>
            stmt.execute(s)
            ()
      catch case e => throw GraceException(e.getMessage, e)

enum DBIO[+A](val underlying: String):
  case Query[+A](val query: String, val parse: ResultSet => A)
      extends DBIO[A](query)
  case Update(val stmt: String) extends DBIO[Int](stmt)
  case Statement(val stmt: String) extends DBIO[Unit](stmt)
  override def toString() = underlying

object DBIO:
  def query[A](query: String, parse: ResultSet => A): DBIO[A] =
    DBIO.Query(query, parse)
  def update(stmt: String): DBIO[Int] = DBIO.Update(stmt)
  def statement(stmt: String): DBIO[Unit] = DBIO.Statement(stmt)

trait JDBCQueryContext[V, S[+X] <: Iterable[X]]
    extends QueryContext[[x] =>> Table[V, x], S]:

  final type Native[+A] = DBIO[A]

  final type Connection = JConnection      