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
  val users = Table[Nothing, User]("users")

  //To enable the `native` string context syntax
  given Capabilities[[x] =>> Tree] with {
    def fromNative[A](bin: Tree): A = ???
    def toNative[A](a: A): Tree = ???
  }

  s"""
  Using raw SQL, I
  """ should "be able to parse a select query from a single table" in {
    println(native"SELECT * FROM $users")
  }
}
