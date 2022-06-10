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
import scala.util.Success

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

  def newConnection(): java.sql.Connection = 
      DriverManager.registerDriver(driver)
      DriverManager.getConnection(url, user.orNull, password)

  implicit var connection: java.sql.Connection = null

  val record1s: Table[V, Record1] = Table[V, Record1]()
  val record2s: Table[V, Record2] = Table[V, Record2]()
  val record3s: Table[V, Record3] = Table[V, Record3]()
  val record4s: Table[V, Record4] = Table[V, Record4]()
  val record5s: Table[V, Record5] = Table[V, Record5]()
  val record6s: Table[V, Record6] = Table[V, Record6]()
  val record7s: Table[V, Record7] = Table[V, Record7]()
  val record8s: Table[V, Record8] = Table[V, Record8]()
  val record9s: Table[V, Record9] = Table[V, Record9]()  
  val record10s: Table[V, Record10] = Table[V, Record10]()  
  val record11s: Table[V, Record11] = Table[V, Record11]()  
  val record12s: Table[V, Record12] = Table[V, Record12]()  
  val record13s: Table[V, Record13] = Table[V, Record13]()  
  val record14s: Table[V, Record14] = Table[V, Record14]()  
  val record15s: Table[V, Record15] = Table[V, Record15]()  

  before {
    connection = newConnection()     
  }

  after {
    connection.close()
  }

  inline def withCleanup[A](block: => A)(using ctx: JDBCQueryContext[V, S]): A =
    def clean() = 
      // delete tables if any exists
      List(
        vsql.tried {record1s.delete()}.toOption,
        vsql.tried {record2s.delete()}.toOption,
        vsql.tried {record3s.delete()}.toOption,
        vsql.tried {record4s.delete()}.toOption,
        vsql.tried {record5s.delete()}.toOption,
        vsql.tried {record6s.delete()}.toOption,
        vsql.tried {record7s.delete()}.toOption,
        vsql.tried {record8s.delete()}.toOption,
        vsql.tried {record9s.delete()}.toOption,
        vsql.tried {record10s.delete()}.toOption,
        vsql.tried {record11s.delete()}.toOption,
        vsql.tried {record12s.delete()}.toOption,
        vsql.tried {record13s.delete()}.toOption,
        vsql.tried {record14s.delete()}.toOption,
        vsql.tried {record15s.delete()}.toOption
      ).flatten.flatMap(_.as[Option].toList)

    clean()
    try 
      block
    finally  
      clean()  

  inline def commonDDLTests()(using ctx: JDBCQueryContext[V, S]) =

    s"""
    The high-level JDBC context for $vendor on DDL commands
    """ should "create and drop a simple table (without annotations)" in withCleanup {
        vsql {
          record1s.create()
        }.asTry.isSuccess shouldBe true
        vsql {
          record1s.delete()
        }.asTry.isSuccess shouldBe true
    }

    it should "allow for multi-statement DDL queries" in {
        vsql {
          record1s.create()
          record1s.delete()
          record1s.create()
          record1s.delete()
        }.asTry.isSuccess shouldBe true
    }

    it should "create and drop annotated tables" in {
        vsql {
          record2s.create()
          record3s.create()
          record4s.create()
          record4s.delete()
          record3s.delete()
          record2s.delete()
        }.asTry.isSuccess shouldBe true
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

    it should "not allow foreign key constraints with mismatched types" in {
      vsql.tried {
        record7s.create()
      }.isFailure shouldBe true
    }

    it should "not allow creation of tables with blank schema names" in {
      vsql.tried {
        record10s.create()
      }.isFailure shouldBe true
    }    

    it should "not allow creation of tables with missing schema annotation" in {
      vsql.tried {
        record11s.create()
      }.isFailure shouldBe true
    }

    it should "not allow creation of tables with blank field name(s)" in {
      vsql.tried {
        record12s.create()
      }.isFailure shouldBe true
    }    

    it should "properly handle foreign keys with custom column names during table creation" in {
      vsql {
        record13s.create()
        record14s.create()
        record14s.delete()
        record13s.delete()
      }.asTry.isSuccess shouldBe true
    }

    it should "not allow creation of tables with duplicate column names" in {
      vsql.tried {
        record15s.create()
      }.isFailure shouldBe true
    }        
}

object JDBCSpec:
  // To prevent some bizarre compiler error, classes are defined outside the test suite.
  @schema("record1s")
  case class Record1(field1: Int, field2: String) derives SQLRow

  @schema("record2s")
  case class Record2(@unique @index field1: Int, @unique field2: String) derives SQLRow

  @schema("record3s")
  case class Record3(
      @pk @autoinc field1: Int,
      @fk(classOf[Record2], "field1", OnDelete.Cascade) @unique @index(
        Order.Desc
      ) field2: Int,
      @name("my_field_3") field3: String,
      @index(Order.Asc) field4: Option[Int] = Some(0)
  ) derives SQLRow

  @schema("record4s")
  case class Record4(
      @fk(classOf[Record2], "field1", OnDelete.SetDefault) field1: Int,
      @fk(classOf[Record2], "field1", OnDelete.Restrict) field2: Int,
      @fk(classOf[Record2], "field1", OnDelete.SetNull) field3: Option[
        Int
      ]
  ) derives SQLRow

  @schema("record5s")
  case class Record5(
      @fk(classOf[Record2], "field1", OnDelete.SetNull) field1: Int
  ) derives SQLRow

  @schema("record6s")
  case class Record6(
      @fk(classOf[Record2], "invalid") field1: Int
  ) derives SQLRow
  
  @schema("record7s")
  case class Record7(
      @fk(classOf[Record2], "field1") field1: String
  ) derives SQLRow  

  @schema("record8s")
  case class Record8(
      @pk @unique @index @fk(classOf[Record2], "field1") field1: Int
  ) derives SQLRow  

  @schema("record9s")
  case class Record9(
      @pk @index @unique @fk(classOf[Record2], "field2") field1: String
  ) derives SQLRow  

  @schema("")
  case class Record10(
      field1: String
  ) derives SQLRow  

  case class Record11(
      field1: String
  ) derives SQLRow  

  @schema("record12s")
  case class Record12(
      @name(" ") field1: String
  ) derives SQLRow    

  @schema("record13s")
  case class Record13(
      @pk @name("custom") field1: Int
  ) derives SQLRow      

  @schema("record14s")
  case class Record14(
      @fk(classOf[Record13], "field1") field1: Int
  ) derives SQLRow

  @schema("record15s")
  case class Record15(
      @name("duplicate") field1: Int, @name("duplicate") field2: Int
  ) derives SQLRow