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


sealed trait Modifier extends scala.annotation.StaticAnnotation 

@field  
class PrimaryKey extends Modifier
object PrimaryKey:
  given FromExpr[PrimaryKey] with
    def unapply(expr: Expr[PrimaryKey])(using q: Quotes): Option[PrimaryKey] = 
      Some(PrimaryKey())

@field      
case class ForeignKey(table: Class[_] | Type[_], column: String, onDelete: compiler.OnDelete = compiler.OnDelete.Cascade) extends Modifier
object ForeignKey:
  given FromExpr[ForeignKey] with
    def unapply(expr: Expr[ForeignKey])(using q: Quotes): Option[ForeignKey] = 
      import q.reflect.*

      inline def targetType(e: Expr[Class[_] | Type[_]]): Type[_] =
        e match
          case '{$x: Class[a]} => Type.of[a] 

      expr match
        case '{ForeignKey($c, ${Expr(r)})} => Some(ForeignKey(targetType(c),r))
        case '{new ForeignKey($c, ${Expr(r)})} => Some(ForeignKey(targetType(c), r))
        case '{ForeignKey($c, ${Expr(r)}, ${Expr(d)})} => Some(ForeignKey(targetType(c), r, d))
        case '{new ForeignKey($c, ${Expr(r)}, ${Expr(d)})} => Some(ForeignKey(targetType(c), r, d))
        case _ => None        

@field
class AutoIncrement extends Modifier
object AutoIncrement:
  given FromExpr[AutoIncrement] with
    def unapply(expr: Expr[AutoIncrement])(using q: Quotes): Option[AutoIncrement] = 
      Some(AutoIncrement())  

@field
class Unique extends Modifier
object Unique:
  given FromExpr[Unique] with
    def unapply(expr: Expr[Unique])(using q: Quotes): Option[Unique] = 
      Some(Unique())    

@field
case class Indexed(order: compiler.Order = compiler.Order.Asc) extends Modifier
object Indexed:
  given FromExpr[Indexed] with
    def unapply(expr: Expr[Indexed])(using q: Quotes): Option[Indexed] = 
      expr match
        case '{Indexed()} | '{new Indexed()} => Some(Indexed())   
        case '{Indexed(${Expr(order)})} => Some(Indexed(order))
        case '{new Indexed(${Expr(order)})} => Some(Indexed(order)) 
        case _ => None    

@field
class Name(val name: String) extends scala.annotation.StaticAnnotation
object Name:
  given FromExpr[Name] with
    def unapply(expr: Expr[Name])(using q: Quotes): Option[Name] = 
      import q.reflect.*
      expr match
        case '{new Name(${Expr(name)})} => Some(Name(name))
        case _ => None          