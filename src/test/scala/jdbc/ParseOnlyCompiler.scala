package jdbc

import graceql.context.jdbc.compiler.*

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import scala.annotation.targetName

object ParseOnlyCompiler extends VendorTreeCompiler[GenSQL]:
  import Node.*
  protected def print(tree: Node[Expr, Type])(using q: Quotes): Expr[String] =
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
        val distinctWords = if distinct then List('{ "DISTINCT" }) else Nil
        val columnsWords = List(print(columns))
        val fromWords = from match
          case sub @ As(s: Select[_, _], name) =>
            List('{ "(" + ${ print(s) } + ")" }, '{ "AS" }, Expr(name))
          case sub: Select[_, _] =>
            List('{ "(" + ${ print(from) } + ")" })
          case i => List(print(i))
        val joinWords = joins.map {case (jt, src, on) =>
          val jtStr = jt match
            case JoinType.Inner => "INNER"
            case JoinType.Left => "LEFT"
            case JoinType.Right => "RIGHT"
            case JoinType.Full => "FULL"
            case JoinType.Cross => "CROSS"
          List(Expr(jtStr), Expr("JOIN"), print(src), Expr("ON"), print(on))
        }.flatten
        val whereWords = where.fold(Nil)(i => List('{ "WHERE" }, print(i)))
        val groupByWords = groupBy.fold(List.empty) { case (cs, h) =>
          List(Expr("GROUP"), Expr("BY"), print(cs)) ++ h.map(print).toList
        }
        val orderByWords = orderBy match
          case Nil => Nil
          case cs =>
            val ord = cs.map {
              case (c, Order.Asc)  => '{ ${ print(c) } + " ASC" }
              case (c, Order.Desc) => '{ ${ print(c) } + " DESC" }
            }
            List(
              Expr("ORDER"),
              Expr("BY"),
              '{ ${ Expr.ofSeq(ord) }.mkString(", ") }
            )
        val limitWords =
          limit.fold(List.empty)(l => List(Expr("LIMIT"), print(l)))
        val offsetWords =
          offset.fold(List.empty)(o => List(Expr("OFFSET"), print(o)))
        val words =
          List('{ "SELECT" }) ++ distinctWords ++ columnsWords ++ List('{
            "FROM"
          }) ++ fromWords ++ joinWords ++ whereWords ++ groupByWords ++ orderByWords ++ limitWords ++ offsetWords

        '{ ${ Expr.ofList(words) }.mkString(" ") }
      case Block(stmts) =>
        '{ ${ Expr.ofSeq(stmts.map(print)) }.map(q => s"$q;").mkString(" ") }
      case Star()       => '{ "*" }
      case Tuple(trees) => '{ ${ Expr.ofSeq(trees.map(print)) }.mkString(", ") }
      case As(tree, name) => '{ ${ print(tree) } + " AS " + ${ Expr(name) } }
      case Ref(name) => Expr(name)
      case Column(name) => Expr(name)
      case SelectCol(n, c) => '{${print(n)} + "." + ${print(c)}}
      case Table(name, _) => name
      case Literal(value) =>
        value.asExprOf[scala.Any] match
          case '{ $i: String } => '{ "\"" + $i + "\"" }
          case v               => '{ $v.toString }
      case Dual() => '{ "DUAL" }
      case FunApp(func, args, _) =>
        val encodedArgs = args.map { a =>
          a match
            case Literal(_) | SelectCol(_, _) | FunApp(Func.Custom(_), _, _) | Star() =>
              print(a)
            case _ => '{ "(" + ${ print(a) } + ")" }
        }
        (func, encodedArgs) match
          case (Func.BuiltIn(Symbol.Eq), List(l, r)) =>
            '{ $l + " = " + $r }          
          case (Func.BuiltIn(Symbol.Neq), List(l, r)) =>
            '{ $l + " != " + $r }                      
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
          case (Func.BuiltIn(Symbol.Count), List(arg)) =>
            '{ "COUNT(" + $arg + ")"}
          case (Func.Custom(name), as) =>
            '{
              ${ Expr(name) } + "(" + ${ Expr.ofSeq(as) }.mkString(", ") + ")"
            }
      case Cast(n, tpe) => 
        val typeStr = q.reflect.TypeRepr.of(using tpe).show(using q.reflect.Printer.TypeReprShortCode)
        '{"CAST(" + ${print(n)} + " AS " + ${Expr(typeStr)} + ")"}     
      case DropTable(name) => '{"DROP TABLE " + $name}
      case CreateTable(name, specs) => 
        val specsStr = specs.map {
          case CreateSpec.ColDef(colName ,tpe, mods) => 
            val typeStr = q.reflect.TypeRepr.of(using tpe).show(using q.reflect.Printer.TypeReprShortCode)
            val modsStr = mods.map {
              case ColMod.AutoInc() => '{"AUTO_INCREMENT"}
              case ColMod.Default(v) => '{"DEFAULT " + ${print(Literal(v))} }
              case ColMod.NotNull() => '{"NOT NULL"}
            }
            '{ ${Expr(colName)} + " " + ${Expr(typeStr)} + ${Expr.ofList(modsStr)}.mkString(" ", " ", "")}
          case CreateSpec.PK(columns) => '{"PRIMARY KEY (" + ${Expr(columns.mkString(", "))} + ")"}
          case CreateSpec.FK(localCol, remoteTableName, remoteColName, onDelete) =>
            val onDeleteStr = onDelete match
              case OnDelete.Cascade => '{"CASCADE"}
              case OnDelete.Restrict => '{"RESTRICT"}
              case OnDelete.SetDefault => '{"SET DEFAULT"}
              case OnDelete.SetNull => '{"SET NULL"}
            '{"FOREIGN KEY (" + ${Expr(localCol)} + ") REFERENCES " + $remoteTableName + "(" + ${Expr(remoteColName)} + ") ON DELETE " + $onDeleteStr }
          case CreateSpec.Index(indices) =>
            val indicesStr = indices.map {
              case (c, Order.Asc) => '{ ${Expr(c)} + " ASC" }
              case (c, Order.Desc) => '{ ${Expr(c)} + " DESC" }
            }
            '{"INDEX (" + ${ Expr.ofList(indicesStr) }.mkString(", ") + ")"}
          case CreateSpec.Unique(indices) =>
            val indicesStr = indices.map(Expr(_))
            '{"UNIQUE (" + ${ Expr.ofList(indicesStr) }.mkString(", ") + ")"}            
        }

        '{"CREATE TABLE " + $name + " (" + ${ Expr.ofList(specsStr) }.mkString(", ") + ")"}

  override protected def adaptSupport[S[+X] <: Iterable[X], A](
      tree: Node[Expr, Type]
  )(using q: Quotes, ts: Type[S], ta: Type[A]): Node[Expr, Type] =
    tree.transform.pre { case TypeAnn(tree, _) =>
      tree
    }
