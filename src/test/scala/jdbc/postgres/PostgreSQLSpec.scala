package jdbc.mysql

import jdbc.JDBCSpec

class PostgreSQLSpec
    extends JDBCSpec(
      "PostgreSQL",
      "jdbc:postgresql://postgres:5432/",
      Some("postgres"),
      "root",
      org.postgresql.Driver()
    ) {}
