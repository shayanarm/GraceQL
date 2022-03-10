import scala.quoted.*
import graceql.core.*
import graceql.compiler.Util
import graceql.typelevel.*
import scala.compiletime.summonInline

package object graceql {
  transparent inline def context[R[_],M[_]](using inline ctx: Context[R,M])(inline query: SqlLike[R,M] ?=> Any): Any = 
    ctx(query)
}
