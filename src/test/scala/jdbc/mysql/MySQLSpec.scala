package jdbc.mysql

import graceql.context.jdbc.mysql.*
import graceql.context.jdbc.*
import jdbc.JDBCSpec

class MySQLSpec
    extends JDBCSpec[MySQL](
      "MySQL",
      "jdbc:mysql://mysql:3306/",
      None,
      "root",
      com.mysql.jdbc.Driver()
    ) {

  withConnection {
    runTests()
  }    
}
