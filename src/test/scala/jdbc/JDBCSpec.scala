package jdbc

import org.scalatest._
import flatspec._
import matchers._
import java.sql.*;
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

trait JDBCSpec[V, S[+X] <: Iterable[X]](
    val vendor: String,
    val url: String,
    val user: Option[String],
    val password: String,
    val driver: Driver
) extends AnyFlatSpec
    with should.Matchers
    with BeforeAndAfter {

  @Name("users")
  case class User(id: Int, name: String) derives SQLRow
  @Name("posts")
  case class Post(id: Int, userId: Int) derives SQLRow

  inline def runTests()(using ctx: JDBCQueryContext[V, S]) =
    implicit var connection: ctx.Connection = null

    before {
      DriverManager.registerDriver(driver)
      connection = DriverManager.getConnection(url, user.orNull, password)
    }

    after {
      connection.close()
    }

    val users: Table[V, User] = Table[V, User]()
    val posts: Table[V, Post] = Table[V, Post]()

    s"""
    The high-level api
    """ should "create a simple table (without annotations)" in {
      sql[V, S] {
        users.create()
      }.run
    }
    it should "drop a table" in {
      noException should be thrownBy {
        sql[V, S] {
          users.delete()
        }.run
      }
    }
}
