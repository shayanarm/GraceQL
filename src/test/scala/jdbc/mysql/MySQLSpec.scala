package jdbc.mysql

import jdbc.JDBCSpec

class MySQLSpec
    extends JDBCSpec(
      "MySQL",
      "jdbc:mysql://mysql:3306/",
      None,
      "root"
    ) {}
