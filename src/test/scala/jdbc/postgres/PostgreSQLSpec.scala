package jdbc.postgres
import graceql.context.jdbc.postgres.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class PostgreSQLSpec
    extends JDBCSpec[PostgreSQL, Seq](
      "PostgreSQL",
      "jdbc:postgresql://postgres:5432/",
      "testdb",
      Map.empty,
      Some("postgres"),
      "root",
      org.postgresql.Driver()
    ) {

  expandCommons()

  it should "allow `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY` and `INDEX` constraints on the same column" in withConnection {
    vsql {
      record2s.create()
      record8s.create()
      record9s.create()
    }.asTry.isSuccess shouldBe true
  }
}
