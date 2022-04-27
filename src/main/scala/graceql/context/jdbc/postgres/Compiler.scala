package graceql.context.jdbc.postgres

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.JDBCCompiler


object Compiler extends JDBCCompiler[PostgreSQL]
