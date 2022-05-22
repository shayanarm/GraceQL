package graceql.context

import graceql.{*}

package object jdbc {
  transparent inline def sql[V, S[+X] <: Iterable[X]]
      : context.CallProxy[JDBCQueryContext[V, S]] =
    context[JDBCQueryContext[V, S]]
}
