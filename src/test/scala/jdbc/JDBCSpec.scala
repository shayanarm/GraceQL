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

trait JDBCSpec[V](
    val vendor: String,
    val url: String,
    val user: Option[String],
    val password: String,
    val driver: Driver
)(using qc: QueryContext[[x] =>> Table[V, x], Seq])
    extends AnyFlatSpec
    with should.Matchers {

  def connect(): Connection =
    DriverManager.registerDriver(driver)
    DriverManager.getConnection(url, user.orNull, password)

  def withConnection[A](f: Connection ?=> A): A =
    val c = connect()
    try f(using c)
    catch
      case e =>
        println(e.getMessage)
        throw e
    finally c.close

  s"""
  Connecting to the $vendor vendor using url: ${url}, user: ${user.orNull}, and pass: ${password}
  """ should "succeed" in {
    noException should be thrownBy {
      withConnection { c ?=> }
    }
  }

  case class User(id: Int, name: String)
  case class Post(id: Int, userId: Int)

  inline def runTests()(using ctx: JDBCQueryContext[V, Seq])(using conn: ctx.Connection) =
    val users: Table[V, User] = Table[V, User]("users")
    val posts: Table[V, Post] = Table[V, Post]("posts")
      s"""
        The high-level api
        """ should "create a table" in {
        sql[V, Seq] {
          users.create()
        }.run

        it should "drop a table" in {
          sql[V, Seq] {
            users.delete()
          }
        }.run
    }
}
