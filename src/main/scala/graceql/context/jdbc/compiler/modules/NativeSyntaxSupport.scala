package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.util.CompileOps
import scala.annotation.targetName

object NativeSyntaxSupport extends CompileModule:
  def apply[V, S[+X] <: Iterable[X]](
      recurse: Context => Expr[Any] => Node[Expr, Type],
      nameGen: () => String
  )(ctx: Context)(using
      q: Quotes,
      tv: Type[V],
      ts: Type[S]
  ): PartialFunction[Expr[Any], Node[Expr, Type]] =
    type Q = Queryable[[x] =>> Table[V, x], S, DBIO]
    given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]
    import q.reflect.{
      Select => _,
      Block => _,
      Literal => SLiteral,
      *
    }

    {
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
              e.asTerm.pos
            )
      case '{ $i: t } if ctx.isRegisteredIdent(i.asTerm) =>
        Node.Ref(ctx.refMap(i.asTerm))
      case '{ $a: t } if ctx.literalEncodable(a) =>
        a match
          case '{ $v: graceql.context.jdbc.Table[V, a] } =>
            Node.Table[Expr, Type, a]('{ $v.name }, Type.of[a])
          case _ =>
            Node.Literal[Expr, Type, t](a)
    }

  protected def parseNative(
      args: Seq[Node[Expr, Type]]
  )(sce: Expr[StringContext])(using q: Quotes): Node[Expr, Type] =
    import q.reflect.*
    val sc = sce match
      case '{ StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
      case '{ new StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
    Node.parse(sc)(args).get
