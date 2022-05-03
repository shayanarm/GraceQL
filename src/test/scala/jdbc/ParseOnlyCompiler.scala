package jdbc

import graceql.context.jdbc.VendorTreeCompiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import scala.annotation.targetName

object ParseOnlyCompiler extends VendorTreeCompiler[AnySQL]:
  override protected def compileAll[S[+X] <: Iterable[X], A](
      expr: Expr[A]
  )(using
      q: Quotes,
      ta: Type[A],
      td: Type[Definable],
      ts: Type[S],
      tq: Type[Queryable]
  ): Expr[Tree] =
    import q.reflect.{Tree => _, *}
    val e = preprocess(expr)
    e match
      case '{ (ev: Capabilities[[_] =>> Tree]) ?=>
            ev.fromNative(
              ev.native($sc : StringContext)(using $ns : NativeSupport[[_] =>> Tree])(${ Varargs(args)}: _*)
            ) : a
          } =>  
            encodedTree(parseNative(args)(sc))
      case _ =>
        throw Exception(
          "Only direct 'fromNative' call followed by the 'native' interpolator are allowed for this test spec"
        )

end ParseOnlyCompiler
