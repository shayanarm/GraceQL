package graceql.context.jdbc.mysql

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*

object Compiler extends VendorTreeCompiler[MySQL]:
  val encoders: Encoders = null
