package graceql.context.jdbc

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.util.CompileOps
import scala.annotation.targetName
import scala.collection.immutable.ArraySeq

trait VendorTreeCompiler[V]:
  protected type Queryable[S[+X] <: Iterable[X]] = graceql.core.Queryable[[x] =>> Table[V, x], S, [x] =>> Tree]
  protected type Definable = graceql.core.Definable[[x] =>> Table[V, x], [x] =>> Tree]

  trait Encoders:
   @targetName("booleanLit")
   def lit(l: Expr[Boolean]): Expr[String]
   @targetName("charLit")
   def lit(l: Expr[Char]): Expr[String]
   @targetName("byteLit")
   def lit(l: Expr[Byte]): Expr[String]
   @targetName("shortLit")
   def lit(l: Expr[Short]): Expr[String]
   @targetName("intLit")
   def lit(l: Expr[Int]): Expr[String]
   @targetName("longLit")
   def lit(l: Expr[Long]): Expr[String]
   @targetName("floatLit")
   def lit(l: Expr[Float]): Expr[String]
   @targetName("doubleLit")
   def lit(l: Expr[Double]): Expr[String]
   @targetName("stringLit")
   def lit(l: Expr[String]): Expr[String]


  val encoders: Encoders = null 

  import CompileOps.*
  protected def preprocess[A](e: Expr[A])(using q: Quotes, ta: Type[A]) : Expr[A] = 
    import q.reflect.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    pipe(e.asTerm).asExprOf[A]

  def compileDML[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[S] ?=> A]
  )(using q: Quotes, ta: Type[A], ts: Type[S], tv: Type[V],tc: Type[Queryable]): Expr[Tree] = 
    e match
        case '{(ev: Queryable[S]) ?=> $body(ev): A} => compileAll(body)

  def compileDDL[A](e: Expr[Definable ?=> A])(
      using q: Quotes, ta: Type[A], tv: Type[V], tc: Type[Definable]
  ): Expr[Tree] =
    e match
        case '{(ev: Definable) ?=> $body(ev): A} => compileAll(body)

  protected def compileAll[S[+X] <: Iterable[X], A](expr: Expr[A])(using q: Quotes, ta: Type[A], td: Type[Definable], ts: Type[S],tq: Type[Queryable]): Expr[Tree] = 
    val e = preprocess(expr)
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
      case '{$v: graceql.context.jdbc.Table[V, a]} => Node.Table[Expr,Type,a]('{$v.name}, Type.of[a])      
    
    SQLParser(args.map(replaceWithValue).toArray)(raw).get

  protected def encodedTree(node: Node[Expr, Type])(using q: Quotes): Expr[Tree] = 
    import q.reflect.{Tree => _, Select => _}
    import Node.*
    node match
      case Select(distinct, columns, from, joins, where, groupBy, orderBy, offset, limit) =>
        '{
          Select(
            ${if distinct then 'true else 'false}, 
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
      // case 

