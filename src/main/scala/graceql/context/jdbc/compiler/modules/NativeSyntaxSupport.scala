package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps
import scala.annotation.targetName
import scala.util.Failure
import scala.util.Success
import graceql.data.Applicative.pure

class NativeSyntaxSupport[V, S[+X] <: Iterable[X]](using override val q: Quotes, tv: Type[V], ts: Type[S]) extends CompileModule[V, S](using q, tv, ts):
  import graceql.data.Validated
  import Validated.*
  def apply(
      recurse: Context => Expr[Any] => Result[Node[Expr, Type]],
      nameGen: () => String
  )(ctx: Context): PartialFunction[Expr[Any], Result[Node[Expr, Type]]] =
    import q.reflect.{
      Select => _,
      Block => SBlock,
      Literal => SLiteral,
      *
    }

    {
      case '{ $i: t } if ctx.isRegisteredIdent(i.asTerm) =>
        Node.Ref(ctx.refMap(i.asTerm)).pure
      case '{ $a: t } if ctx.literalEncodable(a) =>
        a match
          case '{$c: Class[t]} => Node.TypeLit(Type.of[t]).pure
          case '{ $v: graceql.context.jdbc.Table[V, a] } => 
            tableName[a].map(n => Node.Table[Expr, Type, a](Expr(n), Type.of[a]))            
          case _ =>
            Node.Literal[Expr, Type, t](a).pure
      // Multiple statements Support      
      case '{$stmt: a; $expr: b} => 
        for
          (head, tail) <- recurse(ctx)(stmt) ~ recurse(ctx)(expr)
        yield tail match
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
            for 
              nativeArgs <- args.map(recurse(ctx)).sequence
              parsed <- parseNative(nativeArgs)(sc)
            yield parsed
          case '{
                ($c: Q).typed($native: DBIO[a]): DBIO[b]
              } =>
            recurse(ctx)(native).map(r => Node.TypeAnn(r, Type.of[b]))
          case e =>
              "Native code must only be provided using the `lift` method or the `native` interpolator".err
    }

  protected def parseNative(
      args: Seq[Node[Expr, Type]]
  )(sce: Expr[StringContext]): Result[Node[Expr, Type]] =
    import q.reflect.*
    val sc = sce match
      case '{ StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
      case '{ new StringContext(${ Varargs(Exprs(parts)) }: _*) } =>
        StringContext(parts*)
    Node.parse(sc)(args) match
      case Failure(exception) => s"SQL Tree parse error: ${exception.getMessage()}".err
      case Success(value) => value.pure
    
