package graceql.context.jdbc

import scala.quoted.*
import scala.annotation.meta.field

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
class name(val value: String) extends modifier
object name:
  given FromExpr[name] with
    def unapply(expr: Expr[name])(using q: Quotes): Option[name] = 
      import q.reflect.*
      expr match
        case '{new graceql.context.jdbc.name(${Expr(value)})} => Some(name(value))
        case _ => None    

@field
case class schema(val name: String, val compositeUniques: String*) extends scala.annotation.StaticAnnotation 
object schema:
  given FromExpr[schema] with
    def unapply(expr: Expr[schema])(using q: Quotes): Option[schema] = 
      import q.reflect.*
      expr match
        case '{new graceql.context.jdbc.schema(${Expr(value)}, ${ Varargs(Exprs(uniques))}: _*)} => Some(schema(value, uniques*))
        case '{graceql.context.jdbc.schema(${Expr(value)}, ${ Varargs(Exprs(uniques))}: _*)} => Some(schema(value, uniques*))
        case '{new graceql.context.jdbc.schema(${Expr(value)})} => Some(schema(value))
        case '{graceql.context.jdbc.schema(${Expr(value)})} => Some(schema(value))
        case _ => None            