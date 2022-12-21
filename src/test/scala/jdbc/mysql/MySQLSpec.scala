package jdbc.mysql

import graceql.context.jdbc.mysql.*
import graceql.context.jdbc.*
import jdbc.JdbcSpec

class MySqlSpec
    extends JdbcSpec[MySql, Seq](
      "MySQL",
      "jdbc:mysql://mysql:3306/",
      "testdb",
      Map(
        "allowMultiQueries" -> "true"
      ),
      None,
      "root",
      com.mysql.jdbc.Driver()
    ) {

  expandCommons()

  it should "allow `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY` and `INDEX` constraints on the same (non-string) column" in withConnection {
    vsql {
      record2s.create()
      record8s.create()
    }.asTry.isSuccess shouldBe true
  }

  it should "not allow `String` columns to have `FOREIGN KEY` constraints on them" in withConnection {
    vsql.tried {
      record2s.create()
      record9s.create()
    }.isFailure shouldBe true
  }
}
