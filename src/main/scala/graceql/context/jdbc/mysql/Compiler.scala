package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*

object Compiler extends VendorTreeCompiler[MySQL]:
  val encoders: Encoders = null

  protected def print(tree: Node[Expr, Type])(using Quotes): Expr[String] = ???  
