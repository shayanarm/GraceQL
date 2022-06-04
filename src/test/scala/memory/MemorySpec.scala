package memory

import scala.quoted.*
import graceql.*
import graceql.core.*
import graceql.context.memory.*
import graceql.data.Source
import scala.compiletime.summonInline
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.Promise
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.scalatest._
import flatspec._
import matchers._
import java.util.concurrent.TimeUnit

case class User(name: String = "sdf", age: Int)    

class MemorySpec extends AnyFlatSpec with should.Matchers {

    val ref = IterRef(1,2,3)

    """
    The IterRef context
    """ should "not allow ref.create()" in {    
            """
            query[IterRef,Seq] {
                ref.create()
            }.run    
            """ shouldNot compile
        }

    it should "not allow ref.delete()" in {    
            """
            query[IterRef,Seq] {
                ref.delete()
            }.run    
            """ shouldNot compile
        }

    it should "not allow native syntax" in {    
            """
            query[IterRef,Seq] {
                native"foo"
            }.run    
            """ shouldNot compile
        }
    it should "not allow typing on native syntax" in {    
            """
            query[IterRef,Seq] {
                (() => "sdf").typed[Int]
            }.run    
            """ shouldNot compile
        }
}
