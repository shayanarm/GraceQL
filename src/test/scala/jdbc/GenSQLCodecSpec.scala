package jdbc

import org.scalatest._
import flatspec._
import matchers._
import scala.util.Try
import graceql.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.data.Source
import scala.compiletime.summonInline
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

class GenSQLCodecSpec extends AnyFlatSpec with should.Matchers {
  case class User(id: Int, name: String)
  case class Post(id: Int, userId: Int)
  val users: Table[GenSQL, User] = Table[GenSQL, User]("users")
  val posts: Table[GenSQL, Post] = Table[GenSQL, Post]("posts")

  inline def parseAssert[A](
      inline src: Queryable[[x] =>> Table[GenSQL, x], Iterable, DBIO] ?=> A
  )(expected: String) =
    val exe = query[[x] =>> Table[GenSQL, x], Iterable](src)
    assert(exe.compiled.underlying == expected)

  s"""
  Using raw SQL, the JDBC context
  """ should "parse a select query from a single table" in {
    parseAssert {
      native"SELECT u.* FROM ${users.lift} AS u".unlift
    } {
      "SELECT u.* FROM users AS u"
    }
  }

  it should "parse a multiline query" in {
    parseAssert {
      native"""
            SELECT
            *
            FROM
            ${users.lift}
            AS
            u""".unlift
    } {
      "SELECT * FROM users AS u"
    }
  }

  it should "parse a \"SELECT DISTINCT\" query" in {
    parseAssert {
      native"SELECT DISTINCT * FROM ${users.lift} AS u".unlift
    } {
      "SELECT DISTINCT * FROM users AS u"
    }
  }

  it should "parse any keyword without case sensitivity" in {
    parseAssert {
      native"SeLeCt * fROm ${users.lift} As u".unlift
    } {
      "SELECT * FROM users AS u"
    }
  }

  it should "parse literal columns inside the select clause" in {
    parseAssert {
      native"SELECT ${1.lift}, ${"foo".lift} FROM ${users.lift} AS u".unlift
    } {
      "SELECT 1, \"foo\" FROM users AS u"
    }
  }

  it should "parse named columns inside the select clause" in {
    parseAssert {
      native"SELECT ${1.lift} a1, ${"foo".lift} AS a2 FROM ${users.lift} AS u".unlift
    } {
      "SELECT 1 AS a1, \"foo\" AS a2 FROM users AS u"
    }
  }

  it should "parse multiple statements" in {
    parseAssert {
      native"SELECT * FROM ${users.lift}; SELECT * FROM ${users.lift}".unlift
    } {
      "SELECT * FROM users; SELECT * FROM users;"
    }
  }

  it should "parse a literal integer as valid SQL expression" in {
    parseAssert {
      native"${1.lift}".unlift
    } {
      "1"
    }
  }

  it should "parse a simple typed native query" in {
    parseAssert {
      native"SELECT * FROM DUAL".typed[Unit].unlift
    } {
      "SELECT * FROM DUAL"
    }
  }

  it should "parse parentheses around expressions and nested queries" in {
    parseAssert {
      native"SELECT (${1.lift}) FROM (SELECT * FROM DUAL) AS u".unlift
    } {
      "SELECT 1 FROM (SELECT * FROM DUAL) AS u"
    }
  }

  it should "parse a query without requiring parentheses for the embedded subquery" in {
    parseAssert {
      val sub = native"SELECT * FROM DUAL"
      native"SELECT * FROM (SELECT * FROM ${sub})".unlift
    } {
      "SELECT * FROM (SELECT * FROM (SELECT * FROM DUAL))"
    }
  }

  // it should "have no leftovers after parsing" in {
  //   """
  //     parseAssert {
  //       native"select * from ${users.lift} as u leftover".unlift
  //     } {
  //       "SELECT * FROM users AS u"
  //     }
  //   """ shouldNot compile
  // }

  it should "parse a simple arithmetic expression" in {
    parseAssert {
      native"${2.lift} + ${2.lift}".unlift
    } {
      "2 + 2"
    }
  }

  it should "parse compound expressions with scala's precedence orders in mind" in {
    parseAssert {
      native"${2.lift} + ${2.lift} * ${3.lift} - ${1.lift}".unlift
    } {
      "2 + ((2 * 3) - 1)"
    }

    parseAssert {
      native"${2.lift} + -${1.lift}".unlift
    } {
      "2 + (-1)"
    }

    parseAssert {
      native"${true.lift} and ${false.lift} and ${true.lift} or ${false.lift}".unlift
    } {
      "((true AND false) AND true) OR false"
    }

    parseAssert {
      native"${true.lift} && ${false.lift} and ${true.lift} || ${false.lift}".unlift
    } {
      "(true AND false) AND (true OR false)"
    }
  }

  it should "parse a custom function provided that its name is not a reserved keyword" in {
    parseAssert {
      native"myfunc(${1.lift})".unlift
    } {
      "myfunc(1)"
    }

    // """
    // parseAssert {
    //   native"as(${1.lift})".unlift
    // } {
    //   "as(1)"
    // }
    // """ shouldNot compile
  }

  it should "parse a select statement with WHERE clause" in {
    parseAssert {
      native"select * from ${users.lift} u where ${true.lift}".unlift
    } {
      "SELECT * FROM users AS u WHERE true"
    }
  }

  it should "parse a select statement with JOIN clause" in {
    parseAssert {
      native"select u1.*, u2.* from ${users.lift} u1 cross join ${users.lift} u2 on u1.id = u2.id".unlift
    } {
      "SELECT u1.*, u2.* FROM users AS u1 CROSS JOIN users AS u2 ON u1.id = u2.id"
    }
  }

  it should "parse a select statement with ORDER BY clause" in {
    parseAssert {
      native"select * from ${users.lift} u order by ${1.lift},${2.lift} desc".unlift
    } {
      "SELECT * FROM users AS u ORDER BY 1 ASC, 2 DESC"
    }
  }

  it should "parse a select statement with GROUP BY clause" in {
    parseAssert {
      native"select * from ${users.lift} u group by u.id,u.name".unlift
    } {
      "SELECT * FROM users AS u GROUP BY u.id, u.name"
    }
  }

  it should "parse a select statement with LIMIT and/or OFFSET clause" in {
    parseAssert {
      native"select * from ${users.lift} u LIMIT ${5.lift}".unlift
    } {
      "SELECT * FROM users AS u LIMIT 5"
    }

    parseAssert {
      native"select * from ${users.lift} u offset ${5.lift}".unlift
    } {
      "SELECT * FROM users AS u OFFSET 5"
    }

    parseAssert {
      native"select * from ${users.lift} u limit ${5.lift} offset ${5.lift}".unlift
    } {
      "SELECT * FROM users AS u LIMIT 5 OFFSET 5"
    }

    parseAssert {
      native"select * from ${users.lift} u offset ${5.lift} limit ${5.lift}".unlift
    } {
      "SELECT * FROM users AS u LIMIT 5 OFFSET 5"
    }
  }

  it should "parse a select statement with an aggregate function call" in {
    parseAssert {
      native"select count(*) from ${users.lift}".unlift
    } {
      "SELECT COUNT(*) FROM users"
    }
  }

  it should "parse a cast expression" in {
    parseAssert {
      native"cast(${1.lift} as ${classOf[String].lift})".unlift
    } {
      "CAST(1 AS String)"
    }
  }

  it should "parse a DROP TABLE statement" in {
    parseAssert {
      native"drop table ${users.lift}".unlift
    } {
      "DROP TABLE users"
    }
  }

  it should "parse a comprehensive CREATE TABLE statement" in {
    parseAssert {
      native"""create table ${posts.lift} (
          id ${classOf[Int].lift} auto_increment default ${0.lift},
          userId ${classOf[Int].lift} not null,
          primary key (id),
          foreign key (userId) references ${users.lift}(id) on delete cascade,
          index (id asc, userId desc),
          unique (id, userId)
        )""".unlift
    } {
      "CREATE TABLE posts (id Int AUTO_INCREMENT DEFAULT 0, userId Int NOT NULL, PRIMARY KEY (id), FOREIGN KEY (userId) REFERENCES users(id) ON DELETE CASCADE, INDEX (id ASC, userId DESC), UNIQUE (id, userId))"
    }
  }
}
