package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.TreeOps
import scala.annotation.targetName
import scala.util.Failure
import scala.util.Success
import graceql.syntax.*

trait NativeSyntaxSupport[V, S[+X] <: Iterable[X]] extends CompileModule[V, S]:
  import graceql.data.Validated
  import Validated.*
  abstract override def comp(using ctx: Context): PartialFunction[Expr[Any], Result[Node[Expr, Type]]] =
    import q.reflect.{
      Select => _,
      Block => SBlock,
      Literal => SLiteral,
      *
    }

    super.comp.orElse {
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
          (head, tail) <- comp(stmt) ~ comp(expr)
        yield tail match
            case Node.Block(ns) => Node.Block(head :: ns)
            case n => Node.Block(List(head, n))
      case '{
            ($c: Q).native($sc: StringContext)(${
              Varargs(args)
            }: _*)
          } =>
        for 
          nativeArgs <- args.traverse(comp)
          parsed <- parseNative(nativeArgs)(sc)
        yield parsed
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
    Node.Raw(sc, args.toList).pure
    
