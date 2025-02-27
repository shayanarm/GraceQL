package memory

import scala.quoted.*
import graceql.*
import graceql.core.*
import graceql.context.memory.*
import graceql.syntax.*
import graceql.data.*
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
import scala.util.Failure

class MemorySpec extends AnyFlatSpec with should.Matchers {

  val ref = IterRef(1, 2, 3)

  """
    The IterRef context
    """ should "not allow ref.create()" in {
    query[IterRef, Seq].tried {
      ref.create()
    }.isFailure shouldBe true
  }

  it should "not allow ref.delete()" in {
    query[IterRef, Seq].tried {
      ref.delete()
    }.isFailure shouldBe true
  }

  it should "not allow native syntax" in {
    query[IterRef, Seq].tried {
      native"foo"
    }.isFailure shouldBe true
  }

  it should "execute any tree inside it as is" in {
    val seq = Seq(1, 2, 3)
    query[IterRef, Seq] {
      seq.asSource
      for
        s <- seq.filter(_ => true).map(_ + 2)
        if s == s
      yield s - 2
    }.run shouldBe seq
  }
}
