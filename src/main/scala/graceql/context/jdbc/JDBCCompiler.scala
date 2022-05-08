package graceql.context.jdbc

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName
import scala.collection.immutable.ArraySeq

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

class PolymorphicCompiler[V, S[+X] <: Iterable[X]](val encoders: Encoders)(using
    q: Quotes,
    tv: Type[V],
    ts: Type[S]
):
  type Q = Queryable[[x] =>> Table[V, x], S, DBIO]

  given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

  import q.reflect.{Tree => _, Select => _, Block => _, Literal => _, *}
  import CompileOps.*
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
    val pipe =
      preprocess[Q => A] andThen
        appliedToNull[A] andThen      
        toNative andThen
        encodedTree andThen
        adaptSupport andThen
        toDBIO[A]
    pipe(expr)
  private def appliedToNull[A](expr: Expr[Q => A])(using Type[A]): Expr[A] =
    import q.reflect.*
    expr match
      case '{(c: Q) => $b: A} => b
      case '{(c: Q) => $b(c): A} =>
            val applied = new TreeMap {
              override def transformTerm(term: Term)(owner: Symbol): Term = 
                super.transformTerm(term)(owner) match
                  case id: Ident if id.tpe <:< TypeRepr.of[Q] => 
                    '{PolymorphicCompiler.placeholder[Q]}.asExprOf[Q].asTerm
                  case t => t
            }.transformTerm(expr.asTerm)(Symbol.spliceOwner)
            applied.asExprOf[Q => A] match
              case '{(c: Q) => $b: A} => b

  protected def toNative[A](
      e: Expr[A]
  )(using ta: Type[A]): Node[Expr, Type] =
    import q.reflect.{Tree => _, *}

    e match          
      case '{
            ($c: Q).unlift(
              $block: DBIO[a]
            )
          } =>
        block match
          case '{
                ($c: Q).typed($native: DBIO[a]): DBIO[b]
              } =>
            // ToDo: Add typing information
            toNative[a]('{
              $c.unlift($native)
            })
          case '{
                ($c: Q).native($sc: StringContext)(${
                  Varargs(args)
                }: _*)
              } =>
            val nativeArgs = args.map{
              case '{$a: t} =>
                TypeRepr.of[t].widen.asType match
                  case '[x] => 
                    toNative[x](a.asExprOf[x])
            }
            parseNative(nativeArgs)(sc)
          case invalid =>
            report.errorAndAbort(
              s"""
                  Only native code must be used inside the call to `unlift`
                  ${invalid.asTerm.show(using Printer.TreeAnsiCode)}
                  """,
              block.asTerm.pos
            )
      case '{ $dbio: DBIO[a] } =>
        report.errorAndAbort(
          "Native code must only be used inside the call to `unlift`",
          e.asTerm.pos
        )
      case '{ $a: x } =>
        a match
          case '{$v: Boolean } => Node.Literal(v)
          case '{$v: Char }    => Node.Literal(v)
          case '{$v: Byte }    => Node.Literal(v)
          case '{$v: Short }   => Node.Literal(v)
          case '{$v: Int }     => Node.Literal(v)
          case '{$v: Long }    => Node.Literal(v)
          case '{$v: Float }   => Node.Literal(v)
          case '{$v: Double }  => Node.Literal(v)
          case '{$v: String }  => Node.Literal(v)
          case '{$v: graceql.context.jdbc.Table[V, a] } =>
            Node.Table[Expr, Type, a]('{ $v.name }, Type.of[a])
          case other => throw Exception(TypeRepr.of[x].widen.show(using Printer.TypeReprAnsiCode))            

  protected def parseNative(
      args: Seq[Node[Expr, Type]]
  )(sce: Expr[StringContext]): Node[Expr, Type] =
    import q.reflect.*
    val sc = sce match
      case '{ StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
      case '{ new StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
    Node.parse(sc)(args).get

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
            builder.append(s"FROM ${rec(from)}")
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

object PolymorphicCompiler:
  def placeholder[A]: A = throw GraceException("All references to `placeholder` must be eliminated by the end of compilation!")    