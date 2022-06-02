package graceql.context

import graceql.{*}

package object jdbc {
  export jdbc.compiler.OnDelete
  export jdbc.compiler.Order
  
  transparent inline def sql[V, S[+X] <: Iterable[X]]
      : context.CallProxy[JDBCQueryContext[V, S]] =
    context[JDBCQueryContext[V, S]]
}
