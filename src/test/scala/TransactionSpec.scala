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

class TransactionSpec extends AnyFlatSpec with should.Matchers {

  val conn = summon[DummyImplicit]
  val ref = IterRef[Int]()

  def truncate = context[IterRef, Seq] {
    ref.truncate()
  }.future

  def read = context[IterRef, Seq] {
    ref.asSource.read
  }.future

  def insert = context[IterRef, Seq] {
    ref ++= Seq(1, 2, 3, 4, 5).asSource
  }.future

  // def gatling = Future.sequence(Seq.fill(100)(insert.flatMap(_ => read).flatMap(_ => truncate)))

  val trans =
    for _ <- Future.sequence {
        Seq.fill(100) {
          Future.sequence {
            Seq(
              insert,
              read.map(println),
              truncate
            )
          }
        }
      }
    yield ()
  val r = Await.result(trans, Duration.Inf)
  println(r)
}
