import scala.quoted.*
import graceql.core.*
import graceql.data.{MonadError, RunIn}
import scala.compiletime.summonInline

package object graceql {

  extension [C](connection: C)
    def transaction[T[_]](using
        acid: ACID[C],
        run: RunIn[T],
        me: MonadError[T]
    ): Transaction[T,C,Nothing] =
      Transaction.Continuation(
        () => acid.session(connection),
        c => run(() => acid.open(c)),
        c => run(() => acid.commit(c)),
        c => run(() => acid.rollback(c)),
        me
      )
}
