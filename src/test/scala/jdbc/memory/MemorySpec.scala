package memory

import scala.quoted.*
import graceql.*
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

class MemorySpec extends AnyFlatSpec with should.Matchers {
  val ref = IterRef[Int](1)
  inline def plus2(using q: Capabilities[[x] =>> () => x]) = 
    q.function.unary[Int,Int](a => () => a() + 2)

  val r: Future[Vector[Int]] = for
      i <- query[IterRef, Vector] {
        ref.insertMany(Vector(1,2,3).asSource) {i => i}
      }.as[Future]
    yield i
  println(Await.result(r, Duration.Inf))
}
