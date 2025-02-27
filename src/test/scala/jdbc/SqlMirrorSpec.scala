package jdbc

import org.scalatest._
import flatspec._
import matchers._
import graceql.context.jdbc.*

class SqlMirrorSpec extends AnyFlatSpec with should.Matchers {
  import SqlMirrorSpec.*

  def mirrorEquals[A, M](using rep: SqlMirror.Of[A])(using
      ev: M =:= rep.Mirror
  ): Unit = ()

  """SqlMirror instances""" should "accurately infer the underlying flat SQL representations of Scala types" in {

    """
    mirrorEquals[Int, Int]
    mirrorEquals[Option[(String, String)], (Option[String], Option[String])]
    mirrorEquals[Option[Binary], Option[Boolean]]
    mirrorEquals[ContactInfo, (String, String)]
    mirrorEquals[User, (Int, String, Option[String], Option[String], Boolean)]
    mirrorEquals[Option[
      User
    ], (Option[Int], Option[String], Option[Option[String]], Option[Option[String]], Option[Boolean])]
    """ should compile
  }

  it should "cast Scala types to list and parse them back" in {
    val user = User(
      UserId(1),
      "Shayan",
      Some(ContactInfo("555-234", "shayan@example.com")),
      Binary.One
    )
    val mirror = summon[SqlMirror.Of[User]]
    val dyn = mirror.dynamic.to(user)
    val parsed = mirror.dynamic.from(dyn)
    assert(parsed.get == user)
  }

  it should "implement a pattern extractor to reconstruct types from list of values" in {
    val user = User(
      UserId(1),
      "Shayan",
      Some(ContactInfo("555-234", "shayan@example.com")),
      Binary.Zero
    )
    val mirror = summon[SqlMirror.Of[User]]
    val mirrored = mirror.to(user)
    val dyn = mirror.dynamic.to(user)
    dyn match
      case mirror.dynamic(u, m) =>
        assert(u == user)
        assert(m == mirrored)
      case _ => fail("Reconstructing type from values failed")
  }
}

object SqlMirrorSpec {
  case class ContactInfo(phone: String, email: String) derives SqlRow
  case class UserId(underying: Int) derives SqlValueClass
  case class User(id: UserId, name: String, contactInfo: Option[ContactInfo], flag: Binary)
      derives SqlRow

  enum Binary:
    case Zero, One

  object Binary:
    given SqlMapped[Boolean, Binary] with
      def from(b: Boolean): Binary = if b then Binary.One else Binary.Zero
      def to(b: Binary): Boolean = b == Binary.One
}
