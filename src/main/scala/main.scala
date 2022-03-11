import graceql.backend.troll.*
import scala.quoted.*
import graceql.*
import graceql.backend.memory.*
import graceql.data.Source
import graceql.core.*
import scala.compiletime.summonInline

inline def let[A, B](i: A)(f: A => B) = f(i)

@main def main: Unit =
  println("hello!")
  transparent inline def ints = context[Memory, Seq] {
    let(Seq(1,2,3,2,2,3,4).asSource) {
      _.groupBy(identity).read
    }
  }
  println(ints.compiled.map(identity))
