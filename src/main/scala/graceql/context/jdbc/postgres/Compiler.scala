package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import scala.quoted.*

object Compiler extends VendorTreeCompiler[PostgreSQL]:

  protected def print(tree: Node[Expr, Type])(using Quotes): Expr[String] = ???  