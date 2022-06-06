package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*

object Compiler extends VendorTreeCompiler[MySQL]:
  import Node.*
  protected def binary(recurse: Node[Expr, Type] => Expr[String])(using Quotes): PartialFunction[Node[Expr,Type], Expr[String]] =
    PartialFunction.empty   

  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String] = 
    tpe match
      case '[String] => '{"LONGTEXT"}
      case '[Int] => '{"INT"}

  override protected def adaptSupport[S[+X] <: Iterable[X], A](
      tree: Node[Expr, Type]
  )(using q: Quotes, ts: Type[S], ta: Type[A]): Node[Expr, Type] =
    tree.transform.pre { case TypeAnn(tree, _) =>
      tree
    }
