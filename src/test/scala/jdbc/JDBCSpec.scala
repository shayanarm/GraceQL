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

// To prevent some bizarre compiler error, classes are defined outside the test suite.

@Name("record1s")
case class Record1(field1: Int, field2: String) derives SQLRow

@Name("record2s")
case class Record2(@Indexed field1: Int, field2: String) derives SQLRow

@Name("record3s")
case class Record3(
  @PrimaryKey @AutoIncrement field1: Int, 
  @ForeignKey(classOf[Record2], "field1", OnDelete.Cascade) @Unique @Indexed(Order.Desc) field2: Int,   
  @Name("my_field_3") field3: String,
  @Indexed(Order.Asc) field4: Option[Int] = Some(0)
) derives SQLRow

trait JDBCSpec[V, S[+X] <: Iterable[X]](
    val vendor: String,
    val url: String,
    val user: Option[String],
    val password: String,
    val driver: Driver
) extends AnyFlatSpec
    with should.Matchers
    with BeforeAndAfter {

  inline def runTests()(using ctx: JDBCQueryContext[V, S]) =
    implicit var connection: ctx.Connection = null

    val record1s: Table[V, Record1] = Table[V, Record1]()
    val record2s: Table[V, Record2] = Table[V, Record2]()
    val record3s: Table[V, Record3] = Table[V, Record3]()

    before {
      DriverManager.registerDriver(driver)
      connection = DriverManager.getConnection(url, user.orNull, password)

      //delete tables if any exists
      try
        sql[V, S] {
          record1s.delete()
          record2s.delete()
          record3s.delete()
        }.run
      catch
        case _ => ()                
    }

    after {
      connection.close()
    }

    s"""
    The high-level JDBC context for $vendor
    """ should "create and drop a simple table (without annotations)" in {
      noException should be thrownBy {
        sql[V, S] {
          record1s.create()
        }.run
        sql[V, S] {
          record1s.delete()
        }.run
      }
    }

    it should "allow for multi-statement DDL queries" in {
      noException should be thrownBy {
        sql[V, S] {
          record1s.create()
          record1s.delete()
          record1s.create()
          record1s.delete()          
        }.run
      }
    }

    it should "create and drop annotated tables" in {   
      // noException should be thrownBy {
            sql[V, S] {
              record2s.create()
              record3s.create()
              record3s.delete()
              record2s.delete()              
            }.run         
      // }
    }        
}
