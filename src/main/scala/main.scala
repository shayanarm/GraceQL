import graceql.backend.troll.*
import scala.quoted.*
import graceql.*
import graceql.backend.memory.*
import graceql.data.Source
import graceql.core.*
import scala.compiletime.summonInline
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

inline def query(i: Int) = context[Memory, Seq] {
  Seq(i).asSource.distinct.read
}

@main def main: Unit =
  val i = (Math.random * 100).toInt
  val result = query(i).run()
  println(result)
