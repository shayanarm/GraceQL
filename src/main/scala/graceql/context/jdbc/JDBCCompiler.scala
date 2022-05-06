package graceql.context.jdbc

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName
import scala.collection.immutable.ArraySeq

trait VendorTreeCompiler[V]:
  protected final type Queryable[S[+X] <: Iterable[X]] =
    [n[+_]] =>> graceql.core.Queryable[[x] =>> Table[V, x], S, n]
  protected final type Definable =
    [n[+_]] =>> graceql.core.Definable[[x] =>> Table[V, x], n]

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

  val encoders: Encoders

  import CompileOps.*
  protected def preprocess[A](
      e: Expr[A]
  )(using q: Quotes, ta: Type[A]): Expr[A] =
    import q.reflect.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    pipe(e.asTerm).asExprOf[A]

  def compileDML[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[S][DBIO] ?=> A]
  )(using
      q: Quotes,
      ta: Type[A],
      ts: Type[S],
      tv: Type[V],
      tq: Type[Queryable]
  ): Expr[DBIO[A]] =
    import q.reflect.{Statement => _, *}
    e match
      case '{ (ev: Queryable[S][DBIO]) ?=> $body(ev): A } =>
        compileAll[Queryable[S], S, A](body)
      case _ => throw Exception(e.asTerm.show(using Printer.TreeAnsiCode))

  def compileDDL[A](e: Expr[Definable[DBIO] ?=> A])(using
      q: Quotes,
      ta: Type[A],
      tv: Type[V],
      tc: Type[Definable]
  ): Expr[DBIO[A]] =
    import q.reflect.{Statement => _, *}
    e match
      case '{ (ev: Definable[DBIO]) ?=> $body(ev): A } =>
        compileAll[Definable, Iterable, A](body)
      case _ => throw Exception(e.asTerm.show(using Printer.TreeAnsiCode))

  protected def compileAll[C[X[+_]] <: Capabilities[X], S[+X] <: Iterable[
    X
  ], A](expr: Expr[C[DBIO] => A])(using
      q: Quotes,
      tv: Type[V],
      tc: Type[C],
      ta: Type[A],
      ts: Type[S]
  ): Expr[DBIO[A]] =
    val pipe =
      preprocess[C[DBIO] => A] andThen
        toNative[C, S, C[DBIO] => A] andThen
        encodedTree andThen
        adaptSupport andThen
        toDBIO[A]
    pipe(expr)

  protected def toNative[C[X[+_]] <: Capabilities[X], S[+X] <: Iterable[X], A](
      e: Expr[A]
  )(using
      q: Quotes,
      tv: Type[V],
      tc: Type[C],
      ta: Type[A],
      ts: Type[S]
  ): Node[Expr, Type] =
    import q.reflect.{Tree => _, *}
    e match
      case '{ (ev: C[DBIO]) =>
            ev.fromNative(
              $block(ev): DBIO[a]
            )
          } =>
        block match
          case '{ (ev: C[DBIO]) =>
                ev.typed($native(ev): DBIO[a]): DBIO[b]
              } =>
            // ToDo: Add typing information
            toNative[C, S, C[DBIO] => a]('{ (ev: C[DBIO]) =>
              ev.fromNative($native(ev))
            })
          case '{ (ev: C[DBIO]) =>
                ev.native($sc: StringContext)(using $ns: NativeSupport[DBIO])(${
                  Varargs(args)
                }: _*)
              } =>
            parseNative[C](args.map(stringContextArgToNative[C, S]))(sc)
          case '{ (ev: C[DBIO]) =>
                $invalid(ev): a
              } =>
            report.errorAndAbort(
              "Native code must only be generated inside the call to `fromNative`",
              e.asTerm.pos
            )
      case '{ (ev: C[DBIO]) => $v: Boolean } => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Char }    => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Byte }    => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Short }   => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Int }     => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Long }    => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Float }   => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: Double }  => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: String }  => Node.Literal(v)
      case '{ (ev: C[DBIO]) => $v: graceql.context.jdbc.Table[V, a] } =>
        Node.Table[Expr, Type, a]('{ $v.name }, Type.of[a])
      // case '{(ev: C[DBIO]) => $v: Node[Expr,Type] } => v
  protected def stringContextArgToNative[C[X[+_]] <: Capabilities[X], S[
      +X
  ] <: Iterable[X]](
      arg: Expr[Any]
  )(using q: Quotes, tv: Type[V], tc: Type[C], ts: Type[S]): Node[Expr, Type] =
    arg match
      case '{
            ($ev: C[DBIO]).native($sc: StringContext)(using
              $ns: NativeSupport[DBIO]
            )(${ Varargs(args) }: _*): Any
          } =>
        parseNative[C](args.map(stringContextArgToNative[C, S]))(sc)
      case '{ $e: a } => toNative[C, S, C[DBIO] => a]('{ (ev: C[DBIO]) => $e })

  protected def parseNative[C[X[+_]] <: Capabilities[X]](
      args: Seq[Node[Expr, Type]]
  )(sce: Expr[StringContext])(using
      q: Quotes,
      tv: Type[V],
      tc: Type[C]
  ): Node[Expr, Type] =
    import q.reflect.*
    val sc = sce match
      case '{ StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
      case '{ new StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
    val placeholders = args.indices.map(i => s":$i")
    val raw = sc.raw(placeholders*)
    SQLParser(args.toArray)(raw).get

  protected def encodedTree(node: Node[Expr, Type])(using
      q: Quotes
  ): Expr[Tree] =
    import q.reflect.{Tree => _, Select => _, Block => _, Literal => _, *}
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

  protected def print(tree: Expr[Tree])(using Quotes): Expr[String] =
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
  protected def adaptSupport(tree: Expr[Tree])(using q: Quotes): Expr[Tree] =
    tree
  protected def toDBIO[A](
      tree: Expr[Tree]
  )(using q: Quotes, ta: Type[A]): Expr[DBIO[A]] =
    import q.reflect.{Tree => _, Select => _, Block => _, Statement => _}
    import Node.*
    '{ DBIO.Query(${ print(tree) }, (rs) => ???) }
