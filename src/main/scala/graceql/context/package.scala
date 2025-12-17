package graceql

import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline
import scala.util.Try

package object context {
  class CallProxy[C <: Context[?]] {
    inline def apply[A](using ctx: C)(inline query: ctx.Api ?=> A): ctx.Exe[ctx.Read[A]] = ctx.apply(query)
    inline def tried[A](using ctx: C)(inline query: ctx.Api ?=> A): Try[ctx.Exe[ctx.Read[A]]] = ctx.tried(query)
  }
  transparent inline def apply[C <: Context[?]]: CallProxy[C] = CallProxy[C]()
}
