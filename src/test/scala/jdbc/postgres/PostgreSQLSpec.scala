package jdbc.mysql

import graceql.context.jdbc.postgres.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class PostgreSQLSpec
    extends JDBCSpec[PostgreSQL, Seq](
      "PostgreSQL",
      "jdbc:postgresql://postgres:5432/",
      Some("postgres"),
      "root",
      org.postgresql.Driver()
    ) {

    runTests()
}
