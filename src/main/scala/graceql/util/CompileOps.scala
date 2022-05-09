package graceql.util

import scala.quoted.*
import graceql.core.GraceException

object CompileOps {

  def placeholder[A]: A = throw GraceException("All references to `placeholder` must be eliminated by the end of compilation!")

  def betaReduceAll(using q: Quotes)(e: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
    val mapper = new TreeMap:
      override def transformTerm(term: Term)(owner: Symbol): Term =
        super.transformTerm(Term.betaReduce(term).getOrElse(term))(owner)
    mapper.transformTerm(e)(Symbol.spliceOwner)

  def inlineDefs(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
    def replace(name: String, wit: Term)(block: Term): Term =
      new TreeMap {
        override def transformTerm(term: Term)(owner: Symbol) =
          super.transformTerm(term)(owner) match
            case i@Ident(n) if n == name => wit
            case t                     => t
      }.transformTerm(block)(Symbol.spliceOwner)

    val mapper = new TreeMap:
      override def transformTerm(term: Term)(owner: Symbol): Term =
        super.transformTerm(term)(owner) match
          case Inlined(_, l, e) => transformTerm(Block(l, e))(owner)
          case Block(Nil, e)    => e
          case b @ Block(List(DefDef(n1, _, _, _)), Closure(Ident(n2), _))
              if n1 == n2 =>
            b 
          case Block(h :: t, e) =>
            val block = transformTerm(Block(t, e))(owner)
            h match
              case ValDef(name, _, None) => throw NotImplementedError(name)
              case v @ ValDef(name, _, Some(b)) =>
                if v.symbol.flags.is(Flags.Mutable) then
                  report.errorAndAbort(
                    "Mutable variable declarations inside query are not supported.",
                    v.pos
                  )
                else replace(name, b)(block)
              case d : DefDef =>
                report.errorAndAbort(
                  "Method definitions inside query are not supported.",
                  d.pos
                )
          case other => other
    mapper.transformTerm(term)(Symbol.spliceOwner)

  def appliedToPlaceHolder[A, B](expr: Expr[A => B])(using q: Quotes, ta: Type[A], tb: Type[B]): Expr[B] =
    import q.reflect.*
    val p = '{placeholder[A]}
    Expr.betaReduce('{$expr($p)})  

  def appliedToPlaceHolder[A, B, C](expr: Expr[(A, B) => C])(using q: Quotes, ta: Type[A], tb: Type[B], tc: Type[C]): Expr[C] =
    import q.reflect.*
    val pa = '{placeholder[A]}
    val pb = '{placeholder[B]}
    Expr.betaReduce('{$expr($pa, $pb)})  

  def logged[A](using q: Quotes, ev: A <:< q.reflect.Tree)(
      op: A => A
  ): A => A = tree =>
    import q.reflect.*
    println(
      s"before:\n${ev(tree).show(using Printer.TreeStructure)}\n${ev(tree).show(using Printer.TreeAnsiCode)}\n"
    )
    val trans = op(tree)
    println(
      s"after:\n${ev(trans).show(using Printer.TreeStructure)}\n${ev(trans).show(using Printer.TreeAnsiCode)}\n"
    )
    trans
}
