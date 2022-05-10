package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName

trait VendorTreeCompiler[V]:
  val encoders: Encoders

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
        PolymorphicCompiler[V, S](encoders)
          .compile[A](body)

trait Encoders:
  @targetName("booleanLit")
  def lit(l: Expr[Boolean])(using Quotes): Expr[String]
  @targetName("charLit")
  def lit(l: Expr[Char])(using Quotes): Expr[String]
  @targetName("byteLit")
  def lit(l: Expr[Byte])(using Quotes): Expr[String]
  @targetName("shortLit")
  def lit(l: Expr[Short])(using Quotes): Expr[String]
  @targetName("intLit")
  def lit(l: Expr[Int])(using Quotes): Expr[String]
  @targetName("longLit")
  def lit(l: Expr[Long])(using Quotes): Expr[String]
  @targetName("floatLit")
  def lit(l: Expr[Float])(using Quotes): Expr[String]
  @targetName("doubleLit")
  def lit(l: Expr[Double])(using Quotes): Expr[String]
  @targetName("stringLit")
  def lit(l: Expr[String])(using Quotes): Expr[String]
  def alias(l: Expr[String])(using Quotes): Expr[String]
  def tableName(l: Expr[String])(using Quotes): Expr[String]

class Context(
    val refMap: Map[Any, String] =
      Map.empty[Any, String]
):
  import CompileOps.placeholder
  def withRegisteredIdent[A](using q: Quotes, ta: Type[A])(name: String): (Context, Expr[A]) =
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
          case _ => super.traverseTree(tree)(owner)
    }.traverseTree(expr.asTerm)(Symbol.spliceOwner)
    !encountered

  def newPlaceholder[A](using Quotes, Type[A]): Expr[A] = '{placeholder[A]}  

  def isRegisteredIdent(using q: Quotes)(tree: q.reflect.Tree) =
    refMap.contains(tree)  

  def refName(using q: Quotes)(tree: q.reflect.Tree): String =
    refMap(tree)


abstract class CompileModule:
  def apply[V, S[+X] <: Iterable[X]](
      recurse: Context => Expr[Any] => Node[Expr,Type],
      nameGen: () => String
  )(ctx: Context)(using
      q: Quotes, tv: Type[V], ts: Type[S]
  ): PartialFunction[Expr[Any], Node[Expr, Type]]

class PolymorphicCompiler[V, S[+X] <: Iterable[X]](val encoders: Encoders)(using
    val q: Quotes,
    tv: Type[V],
    ts: Type[S]
):
  type Q = Queryable[[x] =>> Table[V, x], S, DBIO]

  given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

  import q.reflect.{Tree => _, Select => _, Block => _, Literal => SLiteral, *}
  import CompileOps.*
  import PolymorphicCompiler.*
    
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

    val fallback: PartialFunction[Expr[Any], Node[Expr, Type]] = {
        case e => report.errorAndAbort("Unsupported operation!", e.asTerm.pos)
      }  
    def toNative(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]] = 
      partials.foldRight(fallback){(i, c) => 
        i[V,S](toNative, nameGen.nextName)(ctx).orElse(c)
      }

    val pipe =
      appliedToPlaceHolder[Q, A] andThen
        preprocess[A] andThen
        toNative(Context()) andThen
        encodedTree andThen
        adaptSupport andThen
        toDBIO[A]
    pipe(expr)

  protected def encodedTree(node: Node[Expr, Type]): Expr[Tree] =

    import Node.*
    node match
      case Select(
            distinct,
            columns,
            from,
            joins,
            where,
            groupBy,
            orderBy,
            offset,
            limit
          ) =>
        '{
          Select(
            ${ Expr(distinct) },
            ${ encodedTree(columns) },
            ${ encodedTree(from) },
            ${ Expr.ofSeq(joins.map(encodedTree)) }.to(List),
            ${ where.map(encodedTree).fold('{ None }) { i => '{ Some($i) } } },
            ${
              groupBy.map(encodedTree).fold('{ None }) { i => '{ Some($i) } }
            },
            ${ Expr.ofSeq(orderBy.map(encodedTree)) }.to(List),
            ${ offset.map(encodedTree).fold('{ None })(i => '{ Some($i) }) },
            ${ limit.map(encodedTree).fold('{ None })(i => '{ Some($i) }) }
          )
        }
      case Block(stmts) =>
        '{ Block(${ Expr.ofSeq(stmts.map(encodedTree)) }.to(List)) }
      case Dual() => '{ Dual() }
      case Star() => '{ Star() }
      case As(tree, name) =>
        '{ As(${ encodedTree(tree) }, ${ encoders.alias(Expr(name)) }) }
      case Table(name, _) => '{ Table(${ encoders.tableName(name) }, ()) }
      case Tuple(elems) =>
        '{ Tuple(${ Expr.ofSeq(elems.map(encodedTree)) }.to(List)) }
      case Literal(v) =>
        v.asExprOf[Any] match
          case '{ $a: Boolean } => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Char }    => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Byte }    => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Short }   => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Int }     => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Long }    => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Float }   => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: Double }  => '{ Literal(${ encoders.lit(a) }) }
          case '{ $a: String }  => '{ Literal(${ encoders.lit(a) }) }

  protected def print(tree: Expr[Tree]): Expr[String] =
    import Node.*
    '{
      def rec(q: Tree): String =
        q match
          case Select(
                distinct,
                columns,
                from,
                joins,
                where,
                groupBy,
                orderBy,
                offset,
                limit
              ) =>
            val builder = StringBuilder()
            builder.append("SELECT ")
            if distinct then builder.append("DISTINCT ")
            builder.append(rec(columns) + " ")
            val clause = from match
              case sub@ As(s: Select[_,_], name) =>
                s"(${rec(s)}) AS ${name}"
              case sub: Select[_,_] => s"(${rec(from)})"
              case i => rec(from)
            builder.append(s"FROM ${clause}")
            // s"""
            // SELECT ${if distinct then "DISTINCT " else ""}${rec(columns)}
            // FROM ${rec(from)}
            // ${joins.map(rec).mkString("\n")}
            // ${where.fold("")(i => s"WHERE ${rec(i)}")}
            // ${groupBy.fold("")(i => s"GROUP BY ${rec(i)}")}
            // """
            builder.toString
          case Block(stmts)   => stmts.map(rec).map(q => s"$q;").mkString(" ")
          case Star()         => "*"
          case Tuple(trees)   => trees.map(rec).mkString(", ")
          case As(tree, name) => s"${rec(tree)} AS ${name}"
          case Table(name, _) => s"${name}"
          case Literal(value) => value
          case Dual()         => "DUAL"
      rec($tree)
    }
  protected def adaptSupport(tree: Expr[Tree]): Expr[Tree] =
    tree
  protected def toDBIO[A](
      tree: Expr[Tree]
  )(using ta: Type[A]): Expr[DBIO[A]] =
    import Node.*
    '{ DBIO.Query(${ print(tree) }, (rs) => ???) }

object PolymorphicCompiler
