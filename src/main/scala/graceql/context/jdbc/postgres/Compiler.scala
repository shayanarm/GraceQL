package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*

object Compiler extends VendorTreeCompiler[PostgreSQL]:
  val encoders: Encoders = null
