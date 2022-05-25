package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.util.CompileOps
import scala.annotation.targetName

object DDLSupport extends CompileModule:
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
            ($c: Q).delete(
              $table: Table[V, a]
            )()
          } => recurse(ctx)(withImplicit(c)('{ _ ?=>
            native"drop table ${$table.lift}".unlift
          }))
    }
