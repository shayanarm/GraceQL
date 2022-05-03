package jdbc

import org.scalatest._
import flatspec._
import matchers._
import java.sql.*
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

class SQLParsingSpec extends AnyFlatSpec with should.Matchers {
  case class User(name: String)
  val users = Table[AnySQL, User]("users")

  s"""
  Using raw SQL, I
  """ should "be able to parse a select query from a single table" in {
    noException should be thrownBy {
      query[[x] =>> Table[AnySQL, x], Iterable] {
        fromNative {
          native"SELECT * FROM $users AS u"
        }
      }
    }
  }

  it should "be able to parse a multiline query" in {
    noException should be thrownBy {
      query[[x] =>> Table[AnySQL, x], Iterable] {
        fromNative {
          native"""
            SELECT
            *
            FROM
            $users
            AS
            u"""
        }
      }
    }
  }

  it should "be able to parse a \"SELECT DISTINCT\" query" in {
    noException should be thrownBy {
      query[[x] =>> Table[AnySQL, x], Iterable] {
        fromNative {
          native"SELECT DISTINCT * FROM $users AS u"
        }
      }
    }
  }

  it should "be able to parse any keyword without case sensitivity" in {
    noException should be thrownBy {
      query[[x] =>> Table[AnySQL, x], Iterable] {
        fromNative {
          native"SeLeCt * fROm $users As u"
        }
      }
    }
  }
}
