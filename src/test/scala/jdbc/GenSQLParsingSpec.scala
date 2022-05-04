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

class GenSQLParsingSpec extends AnyFlatSpec with should.Matchers {
  case class User(name: String)
  val users = Table[GenSQL, User]("users")

  inline def parseAssert[A](inline src: Capabilities[Statement] ?=> Statement[A])(expected: String) = 
    val stmt = query[[x] =>> Table[GenSQL, x], Iterable] { fromNative { src } }
    assert(stmt.compiled.tree.printer.compact == expected)

  s"""
  Using raw SQL, the JDBC context
  """ should "parse a select query from a single table" in {
      parseAssert { 
        native"SELECT * FROM $users AS u"
      }{
        "SELECT * FROM users AS u"
      }
  }

  it should "parse a multiline query" in {
      parseAssert { 
          native"""
            SELECT
            *
            FROM
            $users
            AS
            u"""
      }{
        "SELECT * FROM users AS u"
      }
  }

  it should "parse a \"SELECT DISTINCT\" query" in {
    parseAssert{
      native"SELECT DISTINCT * FROM $users AS u"
    }{
      "SELECT DISTINCT * FROM users AS u"
    }
  }

  it should "parse any keyword without case sensitivity" in {
    parseAssert {
      native"SeLeCt * fROm $users As u"
    }{
      "SELECT * FROM users AS u"
    }
  }

  it should "parse literal columns inside the select clause" in {
    parseAssert {
      native"SELECT ${1}, ${2} FROM $users AS u"
    }{
      "SELECT 1, 2 FROM users AS u"
    }
  }

  it should "parse named columns inside the select clause" in {
    parseAssert {
      native"SELECT ${1} a1, ${2} AS a2 FROM $users AS u"
    }{
      "SELECT 1 AS a1, 2 AS a2 FROM users AS u"
    }
  }
  
}
