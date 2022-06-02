package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection}
import java.sql.ResultSet
import scala.deriving.*

case class Table[V, +A](name: String)

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

enum Modifier:
  case PrimaryKey extends Modifier
  case ForeignKey[T](field: String, onDelete: compiler.OnDelete = compiler.OnDelete.Cascade) extends Modifier
  case AutoIncrement extends Modifier
  case Unique extends Modifier
  case Indexed(order: compiler.Order = compiler.Order.Asc) extends Modifier

object Modifier:
  type Index = Indexed
  //Synonym for Modifier.Indexed
  object Index:
    def apply(order: compiler.Order = compiler.Order.Asc): Modifier = Indexed(order)
    def unapply(mod: Modifier): Option[compiler.Order] = mod match
      case Indexed(o) => Some(o)
      case _ => None

class Name(val name: String) extends scala.annotation.StaticAnnotation
class Modifiers(val modifiers: Modifier*) extends scala.annotation.StaticAnnotation