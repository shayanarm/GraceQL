package jdbc

import org.scalatest._
import flatspec._
import matchers._
import graceql.context.jdbc.*

class SQLMirrorSpec extends AnyFlatSpec with should.Matchers {
  case class ContactInfo(phone: String, email: String) derives SQLRow
  case class UserId(underying: Int) derives SQLValueClass
  case class User(id: UserId, name: String, contactInfo: Option[ContactInfo]) derives SQLRow

  def mirrorEquals[A, M](using rep: SQLMirror.Of[A])(using ev: M =:= rep.Mirror): Unit = ()
  
  """SQLMirror instances""" should "accurately infer the underlying flat SQL representations of Scala types" in {

    """
    mirrorEquals[Int, Int]
    mirrorEquals[Option[(String, String)], (Option[String], Option[String])]
    mirrorEquals[ContactInfo, (String, String)]
    mirrorEquals[User, (Int, String, Option[String], Option[String])]
    mirrorEquals[Option[User], (Option[Int], Option[String], Option[Option[String]], Option[Option[String]])]
    """ should compile
  }

  it should "cast scala types to list and parse them back" in {
    val user = User(UserId(1), "Shayan", Some(ContactInfo("555-234", "shayan@example.com")))
    val mirror = summon[SQLMirror.Of[User]]
    val dyn = mirror.dynamic.to(user)
    val parsed = mirror.dynamic.from(dyn)
    assert(parsed.get == user)
  }

  it should "implement a pattern extractor to reconstruct types from list of values" in {
    val user = User(UserId(1), "Shayan", Some(ContactInfo("555-234", "shayan@example.com")))
    val mirror = summon[SQLMirror.Of[User]]
    val mirrored = mirror.to(user)
    val dyn = mirror.dynamic.to(user)
    dyn match
      case mirror.dynamic(u, m) => 
        assert(u == user)
        assert(m == mirrored)
      case _ => fail("Reconstructing type from values failed")
  }
}
