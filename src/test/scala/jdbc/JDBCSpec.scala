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
  import JDBCSpec.*
  inline def vsql = sql[V, S]
  inline def runTests()(using ctx: JDBCQueryContext[V, S]) =
    implicit var connection: ctx.Connection = null

    val record1s: Table[V, Record1] = Table[V, Record1]()
    val record2s: Table[V, Record2] = Table[V, Record2]()
    val record3s: Table[V, Record3] = Table[V, Record3]()
    val record4s: Table[V, Record4] = Table[V, Record4]()
    val record5s: Table[V, Record5] = Table[V, Record5]()
    val record6s: Table[V, Record6] = Table[V, Record6]()

    before {
      DriverManager.registerDriver(driver)
      connection = DriverManager.getConnection(url, user.orNull, password)

      // delete tables if any exists
      vsql {
        record1s.delete()
      }.as[Try]
      vsql {
        record2s.delete()
      }.as[Try]
      vsql {
        record3s.delete()
      }.as[Try]
      vsql {
        record4s.delete()
      }.as[Try]
    }

    after {
      connection.close()
    }

    s"""
    The high-level JDBC context for $vendor
    """ should "create and drop a simple table (without annotations)" in {
      noException should be thrownBy {
        vsql {
          record1s.create()
        }.run
        vsql {
          record1s.delete()
        }.run
      }
    }

    it should "allow for multi-statement DDL queries" in {
      noException should be thrownBy {
        vsql {
          record1s.create()
          record1s.delete()
          record1s.create()
          record1s.delete()
        }.run
      }
    }

    it should "create and drop annotated tables" in {
      noException should be thrownBy {
        vsql {
          record2s.create()
          record3s.create()
          record4s.create()
          record4s.delete()
          record3s.delete()
          record2s.delete()
        }.run
      }
    }

    it should "not allow OnDelete.SetNull for non-optional columns" in {
      vsql.tried {
        record5s.create()
      }.isFailure shouldBe true
    }

    it should "not allow invalid reference names for foreign keys " in {
      vsql.tried {
        record6s.create()
      }.isFailure shouldBe true
    }
}

object JDBCSpec:
  // To prevent some bizarre compiler error, classes are defined outside the test suite.
  @Name("record1s")
  case class Record1(field1: Int, field2: String) derives SQLRow

  @Name("record2s")
  case class Record2(@Unique @Index field1: Int, field2: String) derives SQLRow

  @Name("record3s")
  case class Record3(
      @PrimaryKey @AutoIncrement field1: Int,
      @ForeignKey(classOf[Record2], "field1", OnDelete.Cascade) @Unique @Index(
        Order.Desc
      ) field2: Int,
      @Name("my_field_3") field3: String,
      @Index(Order.Asc) field4: Option[Int] = Some(0)
  ) derives SQLRow

  @Name("record4s")
  case class Record4(
      @ForeignKey(classOf[Record2], "field1", OnDelete.SetDefault) field1: Int,
      @ForeignKey(classOf[Record2], "field1", OnDelete.Restrict) field2: Int,
      @ForeignKey(classOf[Record2], "field1", OnDelete.SetNull) field3: Option[
        Int
      ]
  ) derives SQLRow

  @Name("record5s")
  case class Record5(
      @ForeignKey(classOf[Record2], "field1", OnDelete.SetNull) field1: Int
  ) derives SQLRow

  @Name("record6s")
  case class Record6(
      @ForeignKey(classOf[Record2], "invalid") field1: Int
  ) derives SQLRow
