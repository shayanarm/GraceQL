package jdbc

import org.scalatest._
import flatspec._
import matchers._
import graceql.context.jdbc.*

class SQLRepSpec extends AnyFlatSpec with should.Matchers {
  case class User(id: Int, name: String) derives SQLRow
  case class UserId(underying: Int) derives SQLValueClass

  val row = summon[SQLRep[User]].toRow(User(1, "Shayan"))
  println(row)
  val user = summon[SQLRep[User]].fromRow(row)
  println(user)
}
