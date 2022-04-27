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

  println {
    for
      _ <- context[IterRef, Seq] {
        ref.update(_ => true) {_ + ref.asSource.size}
      }.asTry
      s <- context[IterRef, Seq] {
        ref.asSource.read
      }.asTry
    yield s
  }
}
