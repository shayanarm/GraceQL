package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*

object Compiler extends VendorTreeCompiler[MySQL]:
  import Node.*

  protected def binary(recurse: Node[Expr, Type] => Expr[String])(using Quotes): PartialFunction[Node[Expr,Type], Expr[String]] =
    {
      case CreateTable(table, Some(specs)) =>
        // MySQL requires key lengths on BLOB/TEXT columns
        def keyName(key: String): String = 
          val tpe = specs.collect {
            case CreateSpec.ColDef(colName ,tpe, mods) if colName == key => tpe 
          }.head //Should not throw as the existence of columns must already have been checked.
          keyLength(typeString(tpe)) match
            case Some(i) => s"$key(${i})"
            case None => key          

        val specStrings = specs.map {
          case CreateSpec.ColDef(colName ,tpe, mods) => 
            val modStrings = mods.collect {
              case ColMod.AutoInc() => '{"AUTO_INCREMENT"}
              case ColMod.Default(v) => '{"DEFAULT " + ${recurse(Literal(v))} }
              case ColMod.NotNull() => '{"NOT NULL"}
            }
            '{ ${Expr(colName)} + " " + ${typeString(tpe)} + ${Expr.ofList(modStrings)}.mkString(" ", " ", "")}
          case CreateSpec.PK(columns) => '{"PRIMARY KEY (" + ${Expr(columns.map(keyName).mkString(", "))} + ")"}
          case CreateSpec.FK(localCol, remoteTableName, remoteColName, onDelete) =>
            val onDeleteStr = onDelete match
              case OnDelete.Cascade => '{"CASCADE"}
              case OnDelete.Restrict => '{"RESTRICT"}
              case OnDelete.SetDefault => '{"SET DEFAULT"}
              case OnDelete.SetNull => '{"SET NULL"}  
            '{
              "FOREIGN KEY (" + ${Expr(keyName(localCol))} + ") REFERENCES " + $remoteTableName + 
              "(" + ${Expr(remoteColName)} + ") ON DELETE " + $onDeleteStr 
            }
          case CreateSpec.Index(indices) =>
            val indexStrings = indices.map { case (idx, ord) =>  
              ord match  
                case Order.Asc => '{ ${Expr(keyName(idx))} + " ASC" }
                case Order.Desc => '{ ${Expr(keyName(idx))} + " DESC" }
            }
            '{"INDEX (" + ${ Expr.ofList(indexStrings) }.mkString(", ") + ")"}
          case CreateSpec.Uniques(indices) =>
            val compUniques = indices.map(i => Expr(keyName(i)))
            '{"UNIQUE (" + ${ Expr.ofList(compUniques) }.mkString(", ") + ")"}              
        }
        val uniqueStrings = specs.collect {
          case CreateSpec.ColDef(colName , _, mods) => 
            mods.collect {
              case ColMod.Unique() => colName
            }
        }.flatten.map {i => 
          '{"UNIQUE (" + ${ Expr(keyName(i)) } + ")"}
        }

        '{"CREATE TABLE " + ${ recurse(table) } + " (" + ${ Expr.ofList(specStrings ++ uniqueStrings) }.mkString(", ") + ")"}     
    }    
  
  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String] = 
    tpe match
      case '[String] => '{"LONGTEXT"}
      case '[Int] => '{"INT"}

  def keyLength(using q: Quotes)(fieldType: Expr[String]): Option[Int] =
    fieldType match
      case '{"LONGTEXT"} => Some(767)
      case _ => None      

  override protected def adaptSupport[S[+X] <: Iterable[X], A](
      tree: Node[Expr, Type]
  )(using q: Quotes, ts: Type[S], ta: Type[A]): Node[Expr, Type] =
    tree.transform.pre { case TypeAnn(tree, _) =>
      tree
    }
