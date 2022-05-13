package jdbc

import graceql.context.jdbc.compiler.*

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import scala.annotation.targetName

object ParseOnlyCompiler extends VendorTreeCompiler[GenSQL]:
  import Node.*
  protected def print(tree: Node[Expr, Type])(using Quotes): Expr[String] =
    tree match
      case Select(
            distinct,
            columns,
            from,
            joins,
            where,
            groupBy,
            orderBy,
            offset,
            limit
          ) =>
        val (fromClause, alias, parens) =
          from match
            case sub @ As(s: Select[_, _], name) =>
              (print(s), '{ Some(${ Expr(name) }) }, '{ true })
            case sub: Select[_, _] => (print(from), '{ None }, '{ true })
            case i                 => (print(from), '{ None }, '{ false })
        '{
          val builder = StringBuilder()
          builder.append("SELECT ")
          if ${ Expr(distinct) } then builder.append("DISTINCT ")
          builder.append(${ print(columns) } + " ")
          val fr = if $parens then s"(${$fromClause})" else $fromClause
          builder.append(s"FROM ${fr}")
          if ${ alias }.isDefined then builder.append(s" AS ${$alias.get}")
          // s"""
          // SELECT ${if distinct then "DISTINCT " else ""}${print(columns)}
          // FROM ${print(from)}
          // ${joins.map(print).mkString("\n")}
          // ${where.fold("")(i => s"WHERE ${print(i)}")}
          // ${groupBy.fold("")(i => s"GROUP BY ${print(i)}")}
          // """
          builder.toString
        }
      case Block(stmts) =>
        '{ ${ Expr.ofSeq(stmts.map(print)) }.map(q => s"$q;").mkString(" ") }
      case Star()       => '{ "*" }
      case Tuple(trees) => '{ ${ Expr.ofSeq(trees.map(print)) }.mkString(", ") }
      case As(tree, name) => '{ ${ print(tree) } + " AS " + ${ Expr(name)} }
      case Table(name, _) => name
      case Literal(value) =>
        value.asExprOf[scala.Any] match
          case '{ $i: String } => '{ "\"" + $i + "\"" }
          case v               => '{ $v.toString }
      case Dual() => '{ "DUAL" }
      case FunApp(func, args, _) =>
        val encodedArgs = args.map { a =>
          a match
            case Literal(_) | SelectCol(_, _) | FunApp(Func.Custom(_), _, _) =>
              print(a)
            case _ => '{ "(" + ${ print(a) } + ")" }
        }
        (func, encodedArgs) match
          case (Func.BuiltIn(Symbol.Plus), List(l, r)) =>
            '{ $l + " + " + $r }
          case (Func.BuiltIn(Symbol.Minus), List(l, r)) =>
            '{ $l + " - " + $r }
          case (Func.BuiltIn(Symbol.Plus), List(l)) =>
            l
          case (Func.BuiltIn(Symbol.Minus), List(l)) =>
            '{ "-" + $l }            
          case (Func.BuiltIn(Symbol.Mult), List(l, r)) =>
            '{ $l + " * " + $r }
          case (Func.BuiltIn(Symbol.And), List(l, r)) =>
            '{ $l + " AND " + $r }
          case (Func.BuiltIn(Symbol.Or), List(l, r)) =>
            '{ $l + " OR " + $r }            
          case (Func.Custom(name), as) =>
            '{ ${ Expr(name) } + "(" + ${Expr.ofSeq(as)}.mkString(", ") + ")" }

  override protected def adaptSupport[S[+X] <: Iterable[X], A](tree: Node[Expr, Type])(using q: Quotes, ts: Type[S], ta: Type[A]): Node[Expr, Type] =
    tree.transform.pre {
      case TypeAnn(tree, _) => tree
    }
