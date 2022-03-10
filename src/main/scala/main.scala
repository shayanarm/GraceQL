import graceql.backend.troll.*
import scala.quoted.*
import graceql.*
import graceql.backend.memory.*
import graceql.data.Source

inline def let[A, B](i: A)(f: A => B) = f(i)

@main def main: Unit =
  println("hello!")
  transparent inline def ints : Any = context[memory, Seq] {
    val src = Vector(1,2,3).asSource
    (src ++ src).read
  }

  val s = summon[Foo[Int]].foo
  s + 2
  println(ints.map(_ + 2))
