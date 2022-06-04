package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection}
import java.sql.ResultSet
import scala.deriving.*
import scala.quoted.*

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

enum Modifier:
  case PrimaryKey extends Modifier
  case ForeignKey[T[_], A](target: T[A],reference: String, onDelete: compiler.OnDelete = compiler.OnDelete.Cascade) extends Modifier
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
  given FromExpr[Modifier] with
    def unapply(expr: Expr[Modifier])(using q: Quotes): Option[Modifier] = 
      import q.reflect.*
      expr match
        case '{PrimaryKey} => Some(PrimaryKey)   
        case '{AutoIncrement} => Some(AutoIncrement)   
        case '{Unique} => Some(Unique)   
        case '{Indexed()} | '{new Indexed()} => Some(Indexed())   
        case '{Indexed($order)} => Expr.unapply(order).map(Indexed(_))
        case '{new Indexed($order)} => Expr.unapply(order).map(Indexed(_))
        case '{ForeignKey($c, $reference) : ForeignKey[_, a]} => Expr.unapply(reference).map(ForeignKey[Type, a](Type.of[a],_))
        case '{new ForeignKey($c, $reference) : ForeignKey[_, a]} => Expr.unapply(reference).map(ForeignKey[Type, a](Type.of[a], _))
        case '{ForeignKey($c, $reference, $onDelete) : ForeignKey[_, a]} => 
          for 
            r <- Expr.unapply(reference)
            d <- Expr.unapply(onDelete)
          yield ForeignKey[Type, a](Type.of[a],r, d)  
        case '{new ForeignKey($c, $reference, $onDelete) : ForeignKey[_, a]} => 
          for 
            r <- Expr.unapply(reference)
            d <- Expr.unapply(onDelete)
          yield ForeignKey[Type, a](Type.of[a], r, d)
        case _ => None    

class Name(val name: String) extends scala.annotation.StaticAnnotation
object Name:
  given FromExpr[Name] with
    def unapply(expr: Expr[Name])(using q: Quotes): Option[Name] = 
      import q.reflect.*
      expr match
        case '{new Name($name)} => Expr.unapply(name).map(Name(_))
        case _ => None

class Modifiers(val values: Modifier*) extends scala.annotation.StaticAnnotation
object Modifiers:
  given FromExpr[Modifiers] with
    def unapply(expr: Expr[Modifiers])(using q: Quotes): Option[Modifiers] = 
      import q.reflect.*
      expr match
        case '{ new Modifiers(${ Varargs(Exprs(parts)) }: _*) } =>
          Some(Modifiers(parts*))
        case _ => None                