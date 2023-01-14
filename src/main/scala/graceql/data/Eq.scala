package graceql.data

import scala.quoted.*

trait Eq[A]:
    def eqv(a: A, b: A): Boolean

object Eq extends LowPriorityGivens:
    given eqType[A](using q: Quotes): Eq[Type[A]] with
        def eqv(ta: Type[A], tb: Type[A]): Boolean = 
            import q.reflect.*
            TypeRepr.of(using ta) =:= TypeRepr.of(using tb)
            
    given tupleBase: Eq[EmptyTuple] with
        def eqv(ta: EmptyTuple, tb: EmptyTuple): Boolean = ta == tb
    
    given tupleInductive[H, T <: Tuple](using evH: Eq[H] ,evT: Eq[T]): Eq[H *: T] with
        def eqv(ta: H *: T, tb: H *: T): Boolean = evH.eqv(ta.head, tb.head) && evT.eqv(ta.tail, tb.tail)

trait LowPriorityGivens:
    given default[A]: Eq[A] with
        def eqv(ta: A, tb: A): Boolean = 
            ta == tb