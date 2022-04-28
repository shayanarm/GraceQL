package graceql

import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline

package object context {
  class CallProxy[C[X[_]] <: Context[X], R[_]] {
    inline def apply[A](using ctx: C[R])(inline query: ctx.Capabilities ?=> A): ctx.Exe[A] = ctx.apply(query)
  }
  transparent inline def apply[C[X[_]] <: Context[X], R[_]]: CallProxy[C, R] = CallProxy[C, R]()
}
