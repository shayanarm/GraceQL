package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*
import graceql.data.Validated
import graceql.data.Validated.*

object Compiler extends VendorTreeCompiler[MySql]:
  import Node.*

  protected def binary(
      recurse: Node[Expr, Type] => Expr[String]
  )(using Quotes): PartialFunction[Node[Expr, Type], Expr[String]] = {
    case CreateTable(table, Some(specs)) =>
      // MySql requires key lengths on BLOB/TEXT columns
      def keyName(key: String): String =
        val tpe = specs.collect {
          case CreateSpec.ColDef(colName, tpe, mods) if colName == key => tpe
        }.head // Should not throw as the existence of columns must already have been checked.
        keyLength(typeString(tpe)) match
          case Some(i) => s"$key(${i})"
          case None    => key

      val specStrings = specs.map {
        case CreateSpec.ColDef(colName, tpe, mods) =>
          val modStrings = mods.collect {
            case ColMod.AutoInc()  => '{ "AUTO_INCREMENT" }
            case ColMod.Default(v) => '{ "DEFAULT " + ${ recurse(Literal(v)) } }
            case ColMod.NotNull()  => '{ "NOT NULL" }
          }
          '{
            ${ Expr(colName) } + " " + ${ typeString(tpe) } + ${
              Expr.ofList(modStrings)
            }.mkString(" ", " ", "")
          }
        case CreateSpec.PK(columns) =>
          '{
            "PRIMARY KEY (" + ${
              Expr(columns.map(keyName).mkString(", "))
            } + ")"
          }
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
              Expr(keyName(localCol))
            } + ") REFERENCES " + $remoteTableName +
              "(" + ${ Expr(remoteColName) } + ") ON DELETE " + $onDeleteStr
          }
        case CreateSpec.Index(indices) =>
          val indexStrings = indices.map { case (idx, ord) =>
            ord match
              case Order.Asc  => '{ ${ Expr(keyName(idx)) } + " ASC" }
              case Order.Desc => '{ ${ Expr(keyName(idx)) } + " DESC" }
          }
          '{ "INDEX (" + ${ Expr.ofList(indexStrings) }.mkString(", ") + ")" }
        case CreateSpec.Uniques(indices) =>
          val compUniques = indices.map(i => Expr(keyName(i)))
          '{ "UNIQUE (" + ${ Expr.ofList(compUniques) }.mkString(", ") + ")" }
      }
      val uniqueStrings = specs
        .collect { case CreateSpec.ColDef(colName, _, mods) =>
          mods.collect { case ColMod.Unique() =>
            colName
          }
        }
        .flatten
        .map { i =>
          '{ "UNIQUE (" + ${ Expr(keyName(i)) } + ")" }
        }

      '{
        "CREATE TABLE " + ${ recurse(table) } + " (" + ${
          Expr.ofList(specStrings ++ uniqueStrings)
        }.mkString(", ") + ")"
      }
  }

  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String] =
    tpe match
      case '[String] => '{ "LONGTEXT" }
      case '[Int]    => '{ "INT" }

  def keyLength(using q: Quotes)(fieldType: Expr[String]): Option[Int] =
    fieldType match
      case '{ "LONGTEXT" } => Some(767)
      case _               => None

  override def delegate[S[+X] <: Iterable[X]](using
      Quotes,
      Type[MySql],
      Type[S]
  ): Delegate[S] =
    new Delegate[S] {
      import q.reflect.*

      override def validateSchema[A](using
          Type[A]
      ): Validated[String, Schema[A]] =
        for
          scheme <- super.validateSchema[A]
          _ <- scheme.fieldSpecs
            .collect {
              case FieldSpec(n, _, mtpe, mods, _)
                  if mtpe.typeRepr.no =:= TypeRepr.of[String] =>
                mods.collectFirst { case _: fk =>
                  (n, mtpe)
                }
            }
            .flatten
            .map((n, tpe) =>
              s"Field ${n} with the underlying type ${Type.show(using tpe)} cannot be a foreign key"
            )
            .asErrors(())
        yield scheme

      override def typeCheck(raw: Node[Expr, Type]): Result[Node[Expr, Type]] =
        for tree <- super.typeCheck(raw)
        yield tree.transform.pre { case TypeAnn(tree, _) => tree}
    }
