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
  val ref: IterRef[Seq][Int] = Ref(Seq(1))
  val trans =
    for
      s1 <- context[Eval, Seq] {
        Seq(1,2,3,4,5).asSource.read
      }.as[Future]
      _ <- context[IterRef[Seq], Seq] {
        ref.insertMany(s1.asSource)
      }.as[Future]
      _ <- context[IterRef[Seq], Seq] {
        ref.delete(_ % 2 == 0)
      }.as[Future]
      s2 <- context[IterRef[Seq], Seq] {
        ref.asSource.read
      }.as[Future]
    yield s2
  val r = Await.result(trans, Duration.Inf)
  println(r)
}
