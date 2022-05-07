import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline

package object graceql {
  export graceql.core.Capabilities
  export graceql.core.QueryContext
  export graceql.core.SchemaContext
  export graceql.core.Transaction
  export graceql.core.Transaction.transaction

  transparent inline def query[R[_], M[+_]]: context.CallProxy[[x[_]] =>> QueryContext[x, M], R] = 
    context[[x[_]] =>> QueryContext[x, M], R]

  transparent inline def schema[R[_]]: context.CallProxy[SchemaContext, R] = 
    context[SchemaContext, R]

  transparent inline def dml[R[_], M[+_]] = query[R, M]  
  transparent inline def ddl[R[_]] = schema[R]
}
