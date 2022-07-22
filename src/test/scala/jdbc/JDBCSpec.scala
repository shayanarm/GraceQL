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
import scala.jdk.CollectionConverters.*
import scala.util.Success

trait JDBCSpec[V, S[+X] <: Iterable[X]](
    val vendor: String,
    val url: String,
    val database: String,
    val arguments: Map[String, String],
    val user: Option[String],
    val password: String,
    val driver: Driver
) extends AnyFlatSpec
    with should.Matchers
    with BeforeAndAfter {
  import JDBCSpec.*
  
  DriverManager.registerDriver(driver)    
  val dbUrl = {
    val argString = arguments.map((k,v) => s"$k=$v").mkString("&")
    s"$url$database?$argString"
  }

  inline def vsql = sql[V, S]

  val record1s: Table[V, Record1] = Table()
  val record2s: Table[V, Record2] = Table()
  val record3s: Table[V, Record3] = Table()
  val record4s: Table[V, Record4] = Table()
  val record5s: Table[V, Record5] = Table()
  val record6s: Table[V, Record6] = Table()
  val record7s: Table[V, Record7] = Table()
  val record8s: Table[V, Record8] = Table()
  val record9s: Table[V, Record9] = Table()  
  val record10s: Table[V, Record10] = Table()  
  val record11s: Table[V, Record11] = Table()  
  val record12s: Table[V, Record12] = Table()  
  val record13s: Table[V, Record13] = Table()  
  val record14s: Table[V, Record14] = Table()  
  val record15s: Table[V, Record15] = Table()  
  val record16s: Table[V, Record16] = Table()  
  val record17s: Table[V, Record17] = Table()

  def withCleanup[A](block: Connection ?=> A): A =
        
    def clean() =
      val connection = DriverManager.getConnection(url, user.orNull, password)    
      Try {connection.prepareStatement(s"DROP DATABASE $database").execute()}
      Try {connection.prepareStatement(s"CREATE DATABASE $database").execute()}
      connection.close()
      
    val dbConn = DriverManager.getConnection(dbUrl, user.orNull, password)

    try 
      block(using dbConn)
    finally
      dbConn.close()
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

    it should "allow for multi-statement DDL queries" in withCleanup {
        vsql {
          record1s.create()
          record1s.delete()
          record1s.create()
          record1s.delete()
        }.asTry.isSuccess shouldBe true
    }

    it should "create and drop annotated tables" in withCleanup {
        vsql {
          record2s.create()
          record3s.create()
          record4s.create()
          record4s.delete()
          record3s.delete()
          record2s.delete()
        }.asTry.isSuccess shouldBe true
    }

    it should "not allow OnDelete.SetNull for non-optional columns" in withCleanup {
      vsql.tried {
        record5s.create()
      }.isFailure shouldBe true
    }

    it should "not allow invalid reference names for foreign keys " in withCleanup {
      vsql.tried {
        record6s.create()
      }.isFailure shouldBe true
    }

    it should "not allow foreign key constraints with mismatched types" in withCleanup {
      vsql.tried {
        record7s.create()
      }.isFailure shouldBe true
    }

    it should "not allow creation of tables with blank schema names" in withCleanup {
      vsql.tried {
        record10s.create()
      }.isFailure shouldBe true
    }    

    it should "not allow creation of tables with missing schema annotation" in withCleanup {
      vsql.tried {
        record11s.create()
      }.isFailure shouldBe true
    }

    it should "not allow creation of tables with blank field name(s)" in withCleanup {
      vsql.tried {
        record12s.create()
      }.isFailure shouldBe true
    }    

    it should "properly handle foreign keys with custom column names during table creation" in withCleanup {
      vsql {
        record13s.create()
        record14s.create()
      }.asTry.isSuccess shouldBe true
    }

    it should "not allow creation of tables with duplicate column names" in withCleanup {
      vsql.tried {
        record15s.create()
      }.isFailure shouldBe true
    }        

    it should "create a table with a composite unique key" in withCleanup {
      vsql {
        record16s.create()
      }.asTry.isSuccess shouldBe true
    }

    it should "not allow creation of a table with an invalid field inside its composite unique key" in withCleanup {
      vsql.tried {
        record17s.create()
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

  @schema(name="record16s", compositeUniqueKey= "field1", "field2", "field3")
  case class Record16(
      field1: Int, field2: Int, field3: Int
  ) derives SQLRow  

  @schema(name="record17s", compositeUniqueKey= "field1", "invalid", "field3")
  case class Record17(
      field1: Int, field2: Int, field3: Int
  ) derives SQLRow  