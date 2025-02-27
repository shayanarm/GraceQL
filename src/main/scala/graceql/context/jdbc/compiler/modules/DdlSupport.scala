package graceql.context.jdbc.compiler.modules

import scala.quoted.*
import graceql.core.{Context => _, *}
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.TreeOps
import scala.annotation.targetName

trait DdlSupport[V, S[+X] <: Iterable[X]] extends CompileModule[V, S]:
  abstract override def comp(using ctx: Context): PartialFunction[Expr[Any], Result[Node[Expr, Type]]] =
    import q.reflect.{
      Select => _,
      Block => _,
      Literal => SLiteral,
      *
    }
    super.comp.orElse {
      case '{
            ($c: Q).delete(
              $table: Table[V, a]
            )()
          } => for {
            t <- comp(table)
          } yield Node.DropTable(t)
      case '{
            ($c: Q).create(
              $table: Table[V, a]
            )()
          } => for {
            t <- comp(table)
          } yield Node.CreateTable(t, None) 
    }
