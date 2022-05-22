package jdbc.mysql

import graceql.context.jdbc.postgres.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class PostgreSQLSpec
    extends JDBCSpec[PostgreSQL](
      "PostgreSQL",
      "jdbc:postgresql://postgres:5432/",
      Some("postgres"),
      "root",
      org.postgresql.Driver()
    ) {

  withConnection {
    runTests()
  }    
}
