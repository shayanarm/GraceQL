package jdbc

import org.scalatest._
import flatspec._
import matchers._
import graceql.context.jdbc.*

class SQLRepSpec extends AnyFlatSpec with should.Matchers {
  case class ContactInfo(phone: String, email: String) derives SQLRow
  case class UserId(underying: Int) derives SQLValueClass
  case class User(id: UserId, name: String, contactInfo: Option[ContactInfo]) derives SQLRow

  def testTest[A, M](using rep: SQLMirror.Of[A])(using ev: M =:= rep.Mirror): Unit = ()
  typeTest[User, (Int, String, Option[String], Option[String])]
}
