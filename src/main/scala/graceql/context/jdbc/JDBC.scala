package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection}
import java.sql.ResultSet
import scala.deriving.*
import scala.quoted.*
import scala.annotation.meta.field

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


sealed trait modifier extends scala.annotation.StaticAnnotation 

@field  
class pk extends modifier
object pk:
  given FromExpr[pk] with
    def unapply(expr: Expr[pk])(using q: Quotes): Option[pk] = 
      Some(pk())

@field      
case class fk(table: Class[_] | Type[_], column: String, onDelete: compiler.OnDelete = compiler.OnDelete.Cascade) extends modifier
object fk:
  given FromExpr[fk] with
    def unapply(expr: Expr[fk])(using q: Quotes): Option[fk] = 
      import q.reflect.*

      inline def targetType(e: Expr[Class[_] | Type[_]]): Type[_] =
        e match
          case '{$x: Class[a]} => Type.of[a] 

      expr match
        case '{fk($c, ${Expr(r)})} => Some(fk(targetType(c),r))
        case '{new graceql.context.jdbc.fk($c, ${Expr(r)})} => Some(fk(targetType(c), r))
        case '{fk($c, ${Expr(r)}, ${Expr(d)})} => Some(fk(targetType(c), r, d))
        case '{new graceql.context.jdbc.fk($c, ${Expr(r)}, ${Expr(d)})} => Some(fk(targetType(c), r, d))
        case _ => None        

@field
class autoinc extends modifier
object autoinc:
  given FromExpr[autoinc] with
    def unapply(expr: Expr[autoinc])(using q: Quotes): Option[autoinc] = 
      Some(autoinc())  

@field
class unique extends modifier
object unique:
  given FromExpr[unique] with
    def unapply(expr: Expr[unique])(using q: Quotes): Option[unique] = 
      Some(unique())    

@field
case class index(order: compiler.Order = compiler.Order.Asc) extends modifier
object index:
  given FromExpr[index] with
    def unapply(expr: Expr[index])(using q: Quotes): Option[index] = 
      expr match
        case '{index()} | '{new graceql.context.jdbc.index()} => Some(index())   
        case '{index(${Expr(order)})} => Some(index(order))
        case '{new graceql.context.jdbc.index(${Expr(order)})} => Some(index(order)) 
        case _ => None    

@field
class name(val value: String) extends scala.annotation.StaticAnnotation
object name:
  given FromExpr[name] with
    def unapply(expr: Expr[name])(using q: Quotes): Option[name] = 
      import q.reflect.*
      expr match
        case '{new graceql.context.jdbc.name(${Expr(value)})} => Some(name(value))
        case _ => None          