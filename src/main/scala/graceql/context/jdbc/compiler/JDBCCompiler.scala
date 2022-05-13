package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName

trait VendorTreeCompiler[V]:
  self =>

  def compile[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[[x] =>> Table[V, x], S, DBIO] ?=> A]
  )(using
      q: Quotes,
      ta: Type[A],
      ts: Type[S],
      tv: Type[V]
  ): Expr[DBIO[A]] =
    import q.reflect.{Statement => _, *}
    e match
      case '{ (c: Queryable[[X] =>> Table[V, X], S, DBIO]) ?=> $body(c): A } =>
        Delegate[S].compile[A](body)

  protected def print(tree: Node[Expr, Type])(using Quotes): Expr[String]

  protected def adaptSupport[S[+X] <: Iterable[X], A](tree: Node[Expr, Type])(using q: Quotes, ts: Type[S], ta: Type[A]): Node[Expr, Type] =
    tree
    
  class Delegate[S[+X] <: Iterable[X]](using
      val q: Quotes,
      tv: Type[V],
      ts: Type[S]
  ):
    type Q = Queryable[[x] =>> Table[V, x], S, DBIO]

    given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

    import q.reflect.{Tree => _, Select => _, Block => _, Literal => SLiteral, *}
    import CompileOps.*

    private class NameGenerator(private val prefix: String):
      private var counter = 1
      def nextName(): String =
        synchronized {
          val id = counter
          counter += 1
          s"$prefix$id"
        }
    private val nameGen = NameGenerator("x")

    val partials: Seq[CompileModule] = Seq(
      modules.NativeSyntaxSupport
    )

    protected def preprocess[A](
        e: Expr[A]
    )(using ta: Type[A]): Expr[A] =
      import q.reflect.*
      val pipe =
        inlineDefs andThen
          betaReduceAll andThen
          inlineDefs
      pipe(e.asTerm).asExprOf[A]

    def compile[A](expr: Expr[Q => A])(using
        ta: Type[A]
    ): Expr[DBIO[A]] =

      val fallback: PartialFunction[Expr[Any], Node[Expr, Type]] = { case e =>
        report.errorAndAbort("Unsupported operation!", e.asTerm.pos)
      }
      def toNative(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]] =
        partials.foldRight(fallback) { (i, c) =>
          i[V, S](toNative, nameGen.nextName)(ctx).orElse(c)
        }

      val pipe =
        appliedToPlaceHolder[Q, A] andThen
          preprocess[A] andThen
          toNative(Context()) andThen
          adaptSupport[S,A] andThen
          toDBIO[A]
      pipe(expr)
    protected def toDBIO[A](
        tree: Node[Expr, Type]
    )(using ta: Type[A]): Expr[DBIO[A]] =
      import Node.*
      '{ DBIO.Query(${ print(tree) }, (rs) => ???) }            

trait Encoders:
  def boolean(l: Expr[Boolean])(using Quotes): Expr[String]
  def char(l: Expr[Char])(using Quotes): Expr[String]
  def byte(l: Expr[Byte])(using Quotes): Expr[String]
  def short(l: Expr[Short])(using Quotes): Expr[String]
  def int(l: Expr[Int])(using Quotes): Expr[String]
  def long(l: Expr[Long])(using Quotes): Expr[String]
  def float(l: Expr[Float])(using Quotes): Expr[String]
  def double(l: Expr[Double])(using Quotes): Expr[String]
  def string(l: Expr[String])(using Quotes): Expr[String]
  def alias(l: String)(using Quotes): String
  def tableName(l: Expr[String])(using Quotes): Expr[String]

class Context(
    val refMap: Map[Any, String] = Map.empty[Any, String]
):
  import CompileOps.placeholder
  def withRegisteredIdent[A](using q: Quotes, ta: Type[A])(
      name: String
  ): (Context, Expr[A]) =
    import q.reflect.*
    val ident = newPlaceholder[A]
    (Context(refMap + (ident.asTerm -> name)), ident)

  def literalEncodable(using q: Quotes)(expr: Expr[Any]): Boolean =
    import q.reflect.*
    var encountered = false
    val symbol = Symbol.requiredMethod("graceql.util.CompileOps.placeholder")
    new q.reflect.TreeTraverser {
      override def traverseTree(tree: q.reflect.Tree)(owner: Symbol): Unit =
        tree match
          case i if i.symbol == symbol => encountered = true
          case _                       => super.traverseTree(tree)(owner)
    }.traverseTree(expr.asTerm)(Symbol.spliceOwner)
    !encountered

  def newPlaceholder[A](using Quotes, Type[A]): Expr[A] = '{ placeholder[A] }

  def isRegisteredIdent(using q: Quotes)(tree: q.reflect.Tree) =
    refMap.contains(tree)

  def refName(using q: Quotes)(tree: q.reflect.Tree): String =
    refMap(tree)

abstract class CompileModule:
  def apply[V, S[+X] <: Iterable[X]](
      recurse: Context => Expr[Any] => Node[Expr, Type],
      nameGen: () => String
  )(ctx: Context)(using
      q: Quotes,
      tv: Type[V],
      ts: Type[S]
  ): PartialFunction[Expr[Any], Node[Expr, Type]]
