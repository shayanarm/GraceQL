import scala.quoted.*
import graceql.core.*
import graceql.compiler.Util
import graceql.typelevel.*
import scala.compiletime.summonInline

package object graceql {
  class CallProxy[R[_],M[_]] {
    inline def apply[A](using ctx: Context[R,M])(inline query: SqlLike[R,M] ?=> A): ctx.Exe[A] = ctx.apply(query)
  }
  transparent inline def context[R[_],M[_]]: CallProxy[R, M] = CallProxy[R,M]()
    
}
