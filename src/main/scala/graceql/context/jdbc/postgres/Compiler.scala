package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*

object Compiler extends VendorTreeCompiler[PostgreSql]:

  import Node.*

  protected def binary(
      recurse: Node[Expr, Type] => Expr[String]
  )(using Quotes): PartialFunction[Node[Expr, Type], Expr[String]] = {
    case CreateTable(table, Some(specs)) =>
      val specStrings = specs.collect {
        case CreateSpec.ColDef(colName, tpe, mods) =>
          val modStrings = mods.collect {
            case ColMod.Default(v) => '{ "DEFAULT " + ${ recurse(Literal(v)) } }
            case ColMod.NotNull()  => '{ "NOT NULL" }
            case ColMod.Unique()   => '{ "UNIQUE" }
          }
          val columnType =
            if mods.exists(m => m.isInstanceOf[ColMod.AutoInc[Expr]]) then
              serialColumnType(tpe)
            else typeString(tpe)
          '{
            ${ Expr(colName) } + " " + ${ columnType } + ${
              Expr.ofList(modStrings)
            }.mkString(" ", " ", "")
          }
        case CreateSpec.PK(columns) =>
          '{ "PRIMARY KEY (" + ${ Expr(columns.mkString(", ")) } + ")" }
        case CreateSpec.FK(
              localCol,
              remoteTableName,
              remoteColName,
              onDelete
            ) =>
          val onDeleteStr = onDelete match
            case OnDelete.Cascade    => '{ "CASCADE" }
            case OnDelete.Restrict   => '{ "RESTRICT" }
            case OnDelete.SetDefault => '{ "SET DEFAULT" }
            case OnDelete.SetNull    => '{ "SET NULL" }
          '{
            "FOREIGN KEY (" + ${
              Expr(localCol)
            } + ") REFERENCES " + $remoteTableName + "(" + ${
              Expr(remoteColName)
            } + ") ON DELETE " + $onDeleteStr
          }
        case CreateSpec.Uniques(indices) =>
          val compUniques = indices.map(Expr(_))
          '{ "UNIQUE (" + ${ Expr.ofList(compUniques) }.mkString(", ") + ")" }
      }
      val indexStrings = specs.collect { case CreateSpec.Index(indices) =>
        indices.map { case (c, o) =>
          val orderStr = o match
            case Order.Asc => "ASC"
            case _         => "DESC"
          val Table(tname, _) = table
          '{
            "CREATE INDEX " + ${ tname } + "_" + ${ Expr(c) } + "_" + ${
              Expr(orderStr)
            } + " ON " + ${ recurse(table) } + " (" + ${ Expr(c) } + " " + ${
              Expr(orderStr)
            } + ")"
          }
        }
      }.flatten
      val createTableStr = '{
        "CREATE TABLE " + ${ recurse(table) } + " (" + ${
          Expr.ofList(specStrings)
        }.mkString(", ") + ")"
      }
      '{ ${ Expr.ofList(createTableStr :: indexStrings) }.mkString("; ") }
  }

  def serialColumnType(using q: Quotes)(tpe: Type[_]): Expr[String] =
    tpe match
      case '[Int] => '{ "SERIAL" }
  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String] =
    tpe match
      case '[String] => '{ "TEXT" }
      case '[Int]    => '{ "INT" }

  override def delegate[S[+X] <: Iterable[X]](using
      Quotes,
      Type[PostgreSql],
      Type[S]
  ): Delegate[S] =
    new Delegate[S] {
      import q.reflect.*

      override def typeCheck(raw: Node[Expr, Type]): Result[Node[Expr, Type]] =
        for tree <- super.typeCheck(raw)
        yield tree.transform.pre { case TypeAnn(tree, _) =>
          tree
        }
    }
