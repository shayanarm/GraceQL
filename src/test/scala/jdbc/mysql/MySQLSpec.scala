package jdbc.mysql

import graceql.context.jdbc.mysql.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class MySQLSpec
    extends JDBCSpec[MySQL, Seq](
      "MySQL",
      "jdbc:mysql://mysql:3306/testdb?allowMultiQueries=true",
      None,
      "root",
      com.mysql.jdbc.Driver()
    ) {

    commonDDLTests()

    it should "allow `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY` and `INDEX` constraints on the same column" in {
        vsql {
          record2s.create()
          record8s.create()
          record8s.delete()
          record2s.delete()                              
        }.run
    }        

    it should "not allow String columns to have a `FOREIGN KEY` constraints on them" in {
        vsql.tried {
          record9s.create()                              
        }.isFailure shouldBe true
    }            
}
