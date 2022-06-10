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

    withCleanup {
      it should "allow `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY` and `INDEX` constraints on the same (non-string) column" in {
          vsql {
            record2s.create()
            record8s.create()
            record8s.delete()
            record2s.delete()                              
          }.asTry.isSuccess shouldBe true
      }        

      it should "not allow `String` columns to have `FOREIGN KEY` constraints on them" in {
          vsql.tried {
            record2s.create()
            record9s.create()
            record9s.delete()
            record2s.delete()
          }.isFailure shouldBe true
      }            
    }        
}
