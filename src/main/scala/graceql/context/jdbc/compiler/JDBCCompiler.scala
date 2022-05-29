package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.CompileOps
import scala.annotation.targetName

trait VendorTreeCompiler[V]:
  self =>
  import VendorTreeCompiler.*
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
    import CompileOps.*
    import q.reflect.{Tree => _, Select => _, Block => _, Literal => SLiteral, *}
    type Q = Queryable[[x] =>> Table[V, x], S, DBIO]
    given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

    private class NameGenerator(private val prefix: String):
      private var counter = 1
      def nextName(): String =
        synchronized {
          val id = counter
          counter += 1
          s"$prefix$id"
        }
    private val nameGen = NameGenerator("x")

    val partials: Seq[CompileModule[V, S]] = Seq(
      modules.NativeSyntaxSupport[V, S],
      modules.DDLSupport[V, S]
    )

    def compile[A](expr: Expr[Q => A])(using
        ta: Type[A]
    ): Expr[DBIO[A]] =

      val fallback: PartialFunction[Expr[Any], Node[Expr, Type]] = { case e =>
        report.errorAndAbort(s"Unsupported operation!\n${e.asTerm.show(using Printer.TreeAnsiCode)}", e.asTerm.pos)
      }
      def toNative(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]] =
        partials.foldRight(fallback) { (i, c) =>
          i(toNative, nameGen.nextName)(ctx).orElse(c)
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
      (ta, tree) match
        case ('[Unit], _: DropTable[_,_]) => 
          '{ DBIO.Statement(${ print(tree) }) }.asExprOf[DBIO[A]]
        case _ => 
          '{ DBIO.Query(${ print(tree) }, (rs) => ???) }   

object VendorTreeCompiler:
  def preprocess[A](
      e: Expr[A]
  )(using q: Quotes, ta: Type[A]): Expr[A] =
    import q.reflect.*
    import CompileOps.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    pipe(e.asTerm).asExprOf[A]              

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
    val symbol = Symbol.requiredMethod("graceql.quoted.CompileOps.placeholder")
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

abstract class CompileModule[V, S[+X] <: Iterable[X]](using
      val q: Quotes,
      val tv: Type[V],
      val ts: Type[S]
  ):

  type Q = Queryable[[x] =>> Table[V, x], S, DBIO]
  given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

  def apply(
      recurse: Context => Expr[Any] => Node[Expr, Type],
      nameGen: () => String
  )(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]]

  protected def withImplicit[P, A](p: Expr[P])(f: Expr[P ?=> A])(using Quotes, Type[P], Type[A]): Expr[A] = 
    VendorTreeCompiler.preprocess('{$f(using $p)})

