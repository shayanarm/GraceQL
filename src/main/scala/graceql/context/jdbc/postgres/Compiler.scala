package graceql.context.jdbc.postgres

import graceql.core.*
import graceql.context.jdbc.{Tree, VendorTreeCompiler}

object Compiler extends VendorTreeCompiler[PostgreSQL]:
  val encoders: Encoders = null
