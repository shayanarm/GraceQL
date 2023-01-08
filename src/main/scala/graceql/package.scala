import scala.quoted.*
import graceql.core.*

package object graceql {
  export graceql.core.Api
  export graceql.core.QueryContext
  export graceql.core.Transaction
  export graceql.core.Transaction.transaction

  transparent inline def query[R[_], M[+_]]: context.CallProxy[QueryContext[R, M]] = 
    context[QueryContext[R, M]]
}
