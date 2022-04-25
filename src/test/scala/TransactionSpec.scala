import graceql.context.troll.*
import scala.quoted.*
import graceql.*
import graceql.core.*
import graceql.context.eval.Eval
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
  inline def query() = context[Eval, Seq] {
    Seq(1, 2, 3).asSource.distinct.read
  }

  val conn = summon[DummyImplicit]
  val trans =
    for 
      sess1 <- conn.transaction[Try]
      sess2 <- conn.transaction[Try]
    yield for
      s1 <- query().as[Try](using sess1)
      s2 <- query().as[Try](using sess2)
    yield s1 ++ s2
  println(trans.run())
}
