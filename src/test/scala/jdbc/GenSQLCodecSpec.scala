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
  case class User(name: String)
  val users: Table[GenSQL, User] = Table[GenSQL, User]("users")

  inline def parseAssert[A](
      inline src: Queryable[[x] =>> Table[GenSQL, x], Iterable, DBIO] ?=> A
  )(expected: String) =
    val exe = query[[x] =>> Table[GenSQL, x], Iterable](src)
    assert(exe.compiled.underlying == expected)

  s"""
  Using raw SQL, the JDBC context
  """ should "parse a select query from a single table" in {
    parseAssert {
      native"SELECT * FROM ${users.lift} AS u".unlift
    } {
      "SELECT * FROM users AS u"
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

  it should "parse a query without requiring parenthesis for the embedded subquery" in {
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
}
