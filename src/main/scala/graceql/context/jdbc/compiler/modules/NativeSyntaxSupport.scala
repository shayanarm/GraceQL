package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps
import scala.annotation.targetName

class NativeSyntaxSupport[V, S[+X] <: Iterable[X]](using override val q: Quotes, tv: Type[V], ts: Type[S]) extends CompileModule[V, S](using q, tv, ts):
  def apply(
      recurse: Context => Expr[Any] => Node[Expr, Type],
      nameGen: () => String
  )(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]] =
    import q.reflect.{
      Select => _,
      Block => SBlock,
      Literal => SLiteral,
      *
    }

    {
      case '{ $i: t } if ctx.isRegisteredIdent(i.asTerm) =>
        Node.Ref(ctx.refMap(i.asTerm))
      case '{ $a: t } if ctx.literalEncodable(a) =>
        a match
          case '{$c: Class[t]} => Node.TypeLit(Type.of[t])
          case '{ $v: graceql.context.jdbc.Table[V, a] } =>            
            Node.Table[Expr, Type, a](Expr(require.tableName[a]), Type.of[a])
          case _ =>
            Node.Literal[Expr, Type, t](a)
      // Multiple statements Support      
      case '{$stmt: a; $expr: b} => 
        val head = recurse(ctx)(stmt)
        recurse(ctx)(expr) match
          case Node.Block(ns) => Node.Block(head :: ns)
          case n => Node.Block(List(head, n))              
      case '{
            ($c: Q).unlift(
              $block: DBIO[a]
            )
          } =>
        recurse(ctx)(block)
      case '{ $dbio: DBIO[a] } =>
        dbio match
          case '{ ($c: Q).lift($a: t) } => recurse(ctx)(a)
          case '{
                ($c: Q).native($sc: StringContext)(${
                  Varargs(args)
                }: _*)
              } =>
            val nativeArgs = args.map(recurse(ctx))
            parseNative(nativeArgs)(sc)
          case '{
                ($c: Q).typed($native: DBIO[a]): DBIO[b]
              } =>
            Node.TypeAnn(recurse(ctx)(native), Type.of[b])
          case e =>
            report.errorAndAbort(
              "Native code must only be provided using the `lift` method or the `native` interpolator",
              e
            )          
    }

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
