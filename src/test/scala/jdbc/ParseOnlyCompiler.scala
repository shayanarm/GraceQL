package jdbc

import graceql.context.jdbc.compiler.*

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import scala.annotation.targetName

object ParseOnlyCompiler extends VendorTreeCompiler[GenSql]:
  import Node.*
  protected def binary(recurse: Node[Expr, Type] => Expr[String])(using
      Quotes
  ): PartialFunction[Node[Expr, Type], Expr[String]] =
    PartialFunction.empty

  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String] =
    Expr(
      q.reflect.TypeRepr
        .of(using tpe)
        .show(using q.reflect.Printer.TypeReprShortCode)
    )

  override def delegate[S[+X] <: Iterable[X]](using
      Quotes,
      Type[GenSql],
      Type[S]
  ): Delegate[S] =
    new Delegate[S] {
      import q.reflect.*

      override def typeCheck(raw: Node[Expr, Type]): Result[Node[Expr, Type]] =
        for tree <- super.typeCheck(raw)
        yield tree.transform.pre { case TypeAnn(tree, _) => tree }
    }
