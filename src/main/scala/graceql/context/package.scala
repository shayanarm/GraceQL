package graceql

import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline

package object context {
  class CallProxy[R[_],M[_]] {
    inline def apply[A](using ctx: Context[R,M])(inline query: Queryable[R,M] ?=> A): ctx.Exe[A] = ctx.apply(query)
  }
  transparent inline def apply[R[_],M[_]]: CallProxy[R, M] = CallProxy[R,M]()
}
