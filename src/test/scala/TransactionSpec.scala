import graceql.context.troll.*
import scala.quoted.*
import graceql.*
import graceql.context.eval.*
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
  val trans =
    for
      sess1 <- conn.transaction[Try]
      sess2 <- conn.transaction[Try]
    yield for
      s1 <- context[Eval, Seq] {
        Seq(1, 2, 3)
      }.as[Try](using sess1)
      s2 <- context[Eval, Seq] {
        Seq(4, 5, 6)
      }.as[Try](using sess2)
    yield s1 ++ s2
  println(trans.run())
}
