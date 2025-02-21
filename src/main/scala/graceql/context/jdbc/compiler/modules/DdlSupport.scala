package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.TreeOps
import scala.annotation.targetName

class DdlSupport[V, S[+X] <: Iterable[X]](using override val q: Quotes, tv: Type[V], ts: Type[S]) extends CompileModule[V, S](using q, tv, ts):
  def apply(
      recurse: Context => Expr[Any] => Result[Node[Expr, Type]],
      nameGen: () => String
  )(ctx: Context): PartialFunction[Expr[Any], Result[Node[Expr, Type]]] =
    import q.reflect.{
      Select => _,
      Block => _,
      Literal => SLiteral,
      *
    }

    {
      case '{
            ($c: Q).delete(
              $table: Table[V, a]
            )()
          } => for {
            t <- recurse(ctx)(table)
          } yield Node.DropTable(t)
      case '{
            ($c: Q).create(
              $table: Table[V, a]
            )()
          } => for {
            t <- recurse(ctx)(table)
          } yield Node.CreateTable(t, None)
    }
