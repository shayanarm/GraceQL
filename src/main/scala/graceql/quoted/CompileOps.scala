package graceql.quoted

import scala.quoted.*
import graceql.core.GraceException
import scala.util.{Try, Success, Failure}

object CompileOps {

  def placeholder[A]: A = throw GraceException("All references to `placeholder` must be eliminated by the end of compilation!")

  def betaReduceAll(using q: Quotes)(e: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
    val mapper = new TreeMap:
      override def transformTerm(term: Term)(owner: Symbol): Term =
        super.transformTerm(Term.betaReduce(term).getOrElse(term))(owner)
    mapper.transformTerm(e)(Symbol.spliceOwner)

  def shrinkBlocks(using q: Quotes)(e: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
      new TreeMap {
        override def transformTerm(term: Term)(owner: Symbol) =
          super.transformTerm(term)(owner) match
            case Inlined(_, l, e) => transformTerm(Block(l, e))(owner) 
            case Block(Nil, e)    => transformTerm(e)(owner)
            case term => term
      }.transformTerm(e)(Symbol.spliceOwner)

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
          case Inlined(o, l, e) => transformTerm(Block(l, e))(owner) match
            case Block(l2, e2) => Inlined(o, l2.asInstanceOf[List[Definition]], e2)
            case other => Inlined(o, Nil, other)          
          case b @ Block(List(DefDef(n1, _, _, _)), Closure(Ident(n2), _))
              if n1 == n2 =>
            b 
          case original@ Block(h :: t, e) =>
            val block = transformTerm(Block(t, e))(owner)
            h match
              case v @ ValDef(name, _, Some(b)) if  !v.symbol.flags.is(Flags.Mutable) =>
                replace(name, b)(block)
              case other => 
                block match
                  case Block(stmts, expr) => Block(h :: stmts, expr)
                  case Inlined(_, stmts, expr) => Block(h :: stmts, expr)
                  case o => Block(List(h), o) 
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
