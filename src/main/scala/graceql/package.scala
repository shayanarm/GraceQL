import scala.quoted.*
import graceql.core.*
import scala.compiletime.summonInline

package object graceql {
  export graceql.core.Capabilities
  export graceql.core.QueryContext
  export graceql.core.Transaction
  export graceql.core.Transaction.transaction

  transparent inline def query[R[_], M[+_]]: context.CallProxy[[x[_]] =>> QueryContext[x, M], R] = 
    context[[x[_]] =>> QueryContext[x, M], R]
}
