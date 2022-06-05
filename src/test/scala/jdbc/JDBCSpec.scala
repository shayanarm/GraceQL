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

  @Name("record1s")
  case class Record1(field1: Int, field2: String) derives SQLRow

  @Name("record2s")
  case class Record2(
    @Modifiers(Modifier.PrimaryKey, Modifier.AutoIncrement) field1: Int, 
    @Name("table_1_field1") @Modifiers(Modifier.ForeignKey(classOf[Record1], "field1", OnDelete.Cascade), Modifier.Unique) field2: Int,   
    @Modifiers(Modifier.Indexed(Order.Desc)) field3: String,
    @Modifiers(Modifier.Indexed(Order.Asc)) field4: Option[Int] = Some(0)
  ) derives SQLRow

  inline def runTests()(using ctx: JDBCQueryContext[V, S]) =
    implicit var connection: ctx.Connection = null

    before {
      DriverManager.registerDriver(driver)
      connection = DriverManager.getConnection(url, user.orNull, password)
    }

    after {
      connection.close()
    }

    val record1s: Table[V, Record1] = Table[V, Record1]()
    val record2s: Table[V, Record2] = Table[V, Record2]()

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

    // it should "create and drop a annotated tables" in {   
    //   noException should be thrownBy {
    //     sql[V, S] {
    //       record1s.create()
    //     }.run
    //     sql[V, S] {
    //       record1s.delete()
    //     }.run
    //   }
    // }        
}
