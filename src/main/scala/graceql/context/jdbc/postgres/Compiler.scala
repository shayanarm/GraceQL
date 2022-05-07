package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.*

object Compiler extends VendorTreeCompiler[PostgreSQL]:
  val encoders: Encoders = null
