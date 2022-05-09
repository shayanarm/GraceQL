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
    val dbio = query[[x] =>> Table[GenSQL, x], Iterable] { q ?=> src(using q) }
    assert(dbio.compiled.underlying == expected)

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

  it should "parse a simple arithmetic expression" in {
    parseAssert {
      native"${2.lift} + ${2.lift}".unlift
    } {
      "2 + 2"
    }
  }

  it should "parse a simple typed native query" in {
    parseAssert {
      native"SELECT * FROM DUAL".typed[Unit].unlift
    } {
      "SELECT * FROM DUAL"
    }
  }

  it should "parse nested native queries" in {
    parseAssert {
      val bit = native"SELECT * FROM DUAL"
      native"SELECT * FROM ${native"SELECT * FROM ${bit}"}".unlift
    } {
      "SELECT * FROM SELECT * FROM SELECT * FROM DUAL"
    }
  }
  
  it should "only allow parsing native query followed by unlift" in {
    parseAssert {
      native"select * from dual".typed[Int].unlift
    } {
      "SELECT * FROM DUAL"
    }
  }
}
