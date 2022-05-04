package graceql.context.jdbc

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName
import scala.collection.immutable.ArraySeq

trait VendorTreeCompiler[V]:
  protected final type Queryable[S[+X] <: Iterable[X]] = [n[+_]] =>> graceql.core.Queryable[[x] =>> Table[V, x], S, n]
  protected final type Definable = [n[+_]] =>> graceql.core.Definable[[x] =>> Table[V, x], n]

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
  protected def preprocess[A](e: Expr[A])(using q: Quotes, ta: Type[A]) : Expr[A] = 
    import q.reflect.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    pipe(e.asTerm).asExprOf[A]

  def compileDML[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[S][Statement] ?=> A]
  )(using q: Quotes, ta: Type[A], ts: Type[S], tv: Type[V], tq: Type[Queryable]): Expr[Statement[A]] = 
    import q.reflect.{Statement => _, *}
    e match
        case '{(ev: Queryable[S][Statement]) ?=> $body(ev): A} => compileAll[Queryable[S], S, A](body)
        case _ => throw Exception(e.asTerm.show(using Printer.TreeAnsiCode))

  def compileDDL[A](e: Expr[Definable[Statement] ?=> A])(
      using q: Quotes, ta: Type[A], tv: Type[V], tc: Type[Definable]
  ): Expr[Statement[A]] =
    import q.reflect.{Statement => _, *}
    e match
        case '{(ev: Definable[Statement]) ?=> $body(ev): A} => compileAll[Definable, Iterable, A](body)
        case _ => throw Exception(e.asTerm.show(using Printer.TreeAnsiCode))

  protected def compileAll[C[X[+_]] <: Capabilities[X], S[+X] <: Iterable[X], A](expr: Expr[C[Statement] => A])(using q: Quotes, tv: Type[V], tc: Type[C], ta: Type[A], ts: Type[S]): Expr[Statement[A]] = 
    val e = preprocess(expr)
    (toStatement[A] compose encodedTree compose compileNative[C,S,A])(e)
  
  protected def compileNative[C[X[+_]] <: Capabilities[X], S[+X] <: Iterable[X], A](expr: Expr[C[Statement] => A])(using q: Quotes, tv: Type[V], tc: Type[C], ta: Type[A], ts: Type[S]): Node[Expr,Type] = 
    ???

  protected def parseNative(args: Seq[Expr[Any]])(sce: Expr[StringContext])(using
      q: Quotes, tv: Type[V]
  ): Node[Expr, Type] =
    import q.reflect.*
    val sc = sce match
      case '{ StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
          StringContext(parts*)
      case '{ new StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
          StringContext(parts*) 
    val placeholders = args.indices.map(i => s":$i")
    val raw = sc.raw(placeholders*)
    def replaceWithValue(e: Expr[Any]): Node[Expr,Type] = e match
      case '{$v: Boolean} => Node.Literal(v)
      case '{$v: Char}   => Node.Literal(v)
      case '{$v: Byte}   => Node.Literal(v)
      case '{$v: Short}  => Node.Literal(v)
      case '{$v: Int}    => Node.Literal(v)
      case '{$v: Long}   => Node.Literal(v)
      case '{$v: Float}  => Node.Literal(v)
      case '{$v: Double} => Node.Literal(v)
      case '{$v: String} => Node.Literal(v)
      case '{$v: graceql.context.jdbc.Table[V, a]} => Node.Table[Expr,Type, a]('{$v.name}, Type.of[a])
    SQLParser(args.map(replaceWithValue).toArray)(raw).get

  protected def encodedTree(node: Node[Expr, Type])(using q: Quotes): Expr[Tree] = 
    import q.reflect.{Tree => _, Select => _, Block => _,Literal => _, *}
    import Node.*
    node match
      case Select(distinct, columns, from, joins, where, groupBy, orderBy, offset, limit) =>
        '{
          Select(
            ${Expr(distinct)}, 
            ${encodedTree(columns)},
            ${encodedTree(from)},
            ${Expr.ofSeq(joins.map(encodedTree))}.to(List),
            ${where.map(encodedTree).fold('{None}){i =>'{Some($i)}}},
            ${groupBy.map(encodedTree).fold('{None}){i =>'{Some($i)}}},
            ${Expr.ofSeq(orderBy.map(encodedTree))}.to(List),
            ${offset.map(i => (i, encoders.lit(i))).fold('{None}){(i,c) => '{Some(Encoded($i,() => $c))}}},
            ${limit.map(i => (i, encoders.lit(i))).fold('{None}){(i,c) => '{Some(Encoded($i,() => $c))}}}
          )
      }
      case Block(stmts) => '{ Block(${Expr.ofSeq(stmts.map(encodedTree))}.to(List))}
      case Star() => '{ Star() }
      case As(tree, name) => '{ As(${encodedTree(tree)}, ${encoders.alias(Expr(name))}) }
      case Table(name, _) => '{Table(Encoded($name,() => ${encoders.tableName(name)}),())}
      case Tuple(elems) => '{Tuple(${Expr.ofSeq(elems.map(encodedTree))}.to(List))}
      case Literal(v) => v.asExprOf[Any] match
        case '{$a: Boolean} => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Char}   => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Byte}   => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Short}  => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Int}    => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Long}   => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Float}  => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: Double} => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
        case '{$a: String} => '{Literal(Encoded($a, () => ${encoders.lit(a)}))}
  protected def toStatement[A](tree: Expr[Tree])(using q: Quotes, ta: Type[A]): Expr[Statement[A]] =
    import q.reflect.{Tree => _, Select => _, Block => _, Statement => _}
    import Node.*    
    '{ Statement($tree, (rs) => ???) }  

