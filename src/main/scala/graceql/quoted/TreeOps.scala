package graceql.quoted

import scala.quoted.*
import graceql.core.GraceException
import scala.util.{Try, Success, Failure}

object TreeOps {

  def placeholder[A]: A = throw GraceException("All references to `placeholder` must be eliminated by the end of compilation!")

  def tryEval[A](thunk: => Expr[A])(using q: Quotes, ta: Type[A]): Expr[Try[A]] = 
    import q.reflect.*
    try   
      '{Success($thunk)}
    catch
      case e =>
        '{Failure(GraceException(${Expr(e.getMessage)}))}  

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
            case Block(Nil, e)    => e
            case Inlined(_, Nil, e) => e
            case Inlined(_, l, e) => Block(l, e)
            case term => term
      }.transformTerm(e)(Symbol.spliceOwner)

  def inlineDefs(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
    import q.reflect.*
    def replace(name: String, wit: Term)(block: Term): Term =
      new TreeMap {
        override def transformTerm(term: Term)(owner: Symbol) =
          super.transformTerm(term)(owner) match
            case i@Ident(n) if i.symbol.name == name => wit
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
              case v @ ValDef(name, _, Some(b)) if !v.symbol.flags.is(Flags.Mutable) =>
                replace(name, b)(block)
              case other => 
                block match
                  case Block(stmts, expr) => Block(h :: stmts, expr)
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
  def foldTypeRepr[A](using Quotes)(
      f: PartialFunction[quotes.reflect.TypeRepr, A]
  )(in: quotes.reflect.TypeRepr): List[A] =
    import quotes.reflect.*
    inline def go(a: quotes.reflect.TypeRepr): List[A] =
      foldTypeRepr(f)(a)
    val out = in match
      case TermRef(tpe, n)           => go(tpe)
      case TypeRef(tpe, name)        => Nil
      case _: ConstantType           => Nil
      case SuperType(tpe1, tpe2)     => go(tpe1) ++ go(tpe2)
      case Refinement(tpe1, n, tpe2) => go(tpe1) ++ go(tpe2)
      case AppliedType(rep, args) =>
        go(rep) ++ args.map(go(_)).flatten
      case AnnotatedType(tpe, term) => go(tpe)
      case AndType(l, r)            => go(l) ++ go(r)
      case OrType(l, r)             => go(l) ++ go(r)
      case MatchType(tpe1, tpe2, args) =>
        go(tpe1) ++ go(tpe2) ++ args.map(go(_)).flatten
      case ByNameType(tpe)              => go(tpe)
      case ParamRef(_m, idx)            => Nil
      case _: ThisType                  => Nil
      case _: RecursiveThis             => Nil
      case RecursiveType(tpe)           => go(tpe)
      case MethodType(params, args, rt) => args.map(go(_)).flatten ++ go(rt)
      case PolyType(params, bounds, rt) => bounds.map(go(_)).flatten ++ go(rt)
      case TypeLambda(_, bounds, body)  => bounds.map(go(_)).flatten ++ go(body)
      case MatchCase(tpe1, tpe2)        => go(tpe1) ++ go(tpe2)
      case TypeBounds(tpe1, tpe2)       => go(tpe1) ++ go(tpe2)
      case _: NoPrefix                  => Nil
      case tree => throw MatchError(tree.show(using Printer.TypeReprStructure))

    f.andThen(_ :: out).applyOrElse(in, _ => out)

  def transformTypeRepr(using Quotes)(
      f: PartialFunction[quotes.reflect.TypeRepr, quotes.reflect.TypeRepr]
  )(in: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    inline def go(a: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
      transformTypeRepr(f)(a)
    val out = in match
      case TermRef(tpe, n) =>
        TermRef(go(tpe), n)
      case TypeRef(tpe, name)        => in
      case _: ConstantType           => in
      case SuperType(tpe1, tpe2)     => SuperType(go(tpe1), go(tpe2))
      case Refinement(tpe1, n, tpe2) => Refinement(go(tpe1), n, go(tpe2))
      // case AppliedType(rep, args) =>
      //   AppliedType(
      //     go(rep),
      //     args.map(go(_))
      //   )
      case AnnotatedType(tpe, term) => AnnotatedType(go(tpe), term)
      case AndType(l, r)            => AndType(go(l), go(r))
      case OrType(l, r)             => OrType(go(l), go(r))
      case MatchType(tpe1, tpe2, args) =>
        MatchType(go(tpe1), go(tpe2), args.map(go(_)))
      case ByNameType(tpe)       => ByNameType(go(tpe))
      case ParamRef(binder, idx) => in
      case _: ThisType           => in
      case _: RecursiveThis      => in
      case old @ RecursiveType(tpe) =>
        RecursiveType(rec => go(rebindParams((old, rec))(tpe)))
      case old @ MethodType(ns, args, rt) =>
        MethodType(ns)(
          mt => args.map(a => go(rebindParams((old, mt))(a))),
          mt => go(rebindParams((old, mt))(rt))
        )
      case old @ PolyType(params, bounds, rt) =>
        PolyType(params)(
          pt =>
            bounds.map(b =>
              go(rebindParams((old, pt))(b)).asInstanceOf[TypeBounds]
            ),
          pt => go(rebindParams((old, pt))(rt))
        )
      case old @ TypeLambda(params, bounds, body) =>
        TypeLambda(
          params,
          tl =>
            bounds.map(b =>
              go(rebindParams((old, tl))(b)).asInstanceOf[TypeBounds]
            ),
          tl => go(rebindParams((old, tl))(body))
        )
      case MatchCase(tpe1, tpe2)  => MatchCase(go(tpe1), go(tpe2))
      case TypeBounds(tpe1, tpe2) => TypeBounds(go(tpe1), go(tpe2))
      case _: NoPrefix            => in
      case tree => throw MatchError(tree.show(using Printer.TypeReprStructure))
    f.applyOrElse(out, identity)

  def rebindParams(using
      Quotes
  )(
      pair: (quotes.reflect.PolyType, quotes.reflect.PolyType) |
        (quotes.reflect.MethodType, quotes.reflect.MethodType) |
        (quotes.reflect.TypeLambda, quotes.reflect.TypeLambda) |
        (quotes.reflect.RecursiveType, quotes.reflect.RecursiveType)
  )(
      in: quotes.reflect.TypeRepr
  ): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    val f: PartialFunction[TypeRepr, TypeRepr] = pair match
      case (old, neo: PolyType) => {
        case ref @ ParamRef(binder, idx) if binder == old =>
          neo.param(idx)
      }
      case (old, neo: MethodType) => {
        case ref @ ParamRef(binder, idx) if binder == old =>
          neo.param(idx)
      }

      case (old: TypeLambda, neo: TypeLambda) => {
        case ref @ ParamRef(binder, idx) if binder == old =>
          neo.param(idx)

      }
      case (
            old: RecursiveType,
            neo: RecursiveType
          ) => {
        case rec if rec == old =>
          neo
      }

    transformTypeRepr(f)(in)    
}
