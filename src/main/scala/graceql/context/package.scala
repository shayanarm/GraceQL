package graceql

import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline

package object context {
  class CallProxy[C <: Context[_]] {
    inline def apply[A](using ctx: C)(inline query: ctx.Capabilities ?=> A): ctx.Exe[A] = ctx.apply(query)
  }
  transparent inline def apply[C <: Context[_]]: CallProxy[C] = CallProxy[C]()
}
