package jdbc.postgres
import graceql.context.jdbc.postgres.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class PostgreSQLSpec
    extends JDBCSpec[PostgreSQL, Seq](
      "PostgreSQL",
      "jdbc:postgresql://postgres:5432/testdb",
      Some("postgres"),
      "root",
      org.postgresql.Driver()
    ) {

    commonDDLTests()

    it should "allow `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY` and `INDEX` constraints on the same column" in {
        vsql {
          record2s.create()
          record8s.create()
          record9s.create()           
          record9s.delete()         
          record8s.delete()
          record2s.delete()                              
        }.run
    }    
}
