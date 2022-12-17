package graceql.data

import scala.collection.WithFilter
import java.util.NoSuchElementException

enum Validated[E, +A]:
    case Valid[E, A](value: A) extends Validated[E, A]
    case Invalid[E, A] (head: E, tail: Seq[E] = Seq.empty) extends Validated[E, A]

    def toOption: Option[A] = this match
        case Valid(v) => Some(v)
        case _ => None

    def fold[B](ifInvalid: => B)(f: A => B): B = this match
        case Valid(v) => f(v)
        case _ => ifInvalid

    def getOrElse[B >: A](a: => B): B = this match
        case Valid(v) => v
        case _ => a
    
    def get(): A = getOrElse(throw NoSuchElementException())

    def errors: Seq[E] = this match
        case Valid(v) => Seq.empty
        case Invalid(h, es) => h +: es

    inline def zip[B](other: Validated[E, B]): Validated[E, A ~ B] =
        this.ap(other.map(b => a => (a, b)))    

    inline def ~[B](other: Validated[E, B]): Validated[E, A ~ B] = zip(other)

    inline def ^^[B](f: A => B) = this.map(f)

    def mapError[O](f: E => O): Validated[O, A] =
        this match
            case Invalid(h, t) => Invalid(f(h), t.map(f))
            case other => other.asInstanceOf[Validated[O, A]] 

    def filter(err: A => E)(pred: A => Boolean): Validated[E, A] =
        this match
            case v@ Valid(a) if !pred(a) => Invalid(err(a))
            case other => other

    inline def filter(err: => E)(pred: A => Boolean): Validated[E, A] =
        filter(_ => err)(pred)
    
    inline def filter(pred: A => Boolean)(using conv: Validated.FromString[E]): Validated[E, A] =
        filter(a => conv(s"Predicate for `Validated.filter` failed for value `${a}`"))(pred)

    inline def withFilter(pred: A => Boolean)(using conv: Validated.FromString[E]): Validated[E, A] =
        filter(pred)        
        

object Validated:
    import Validated.*;
        
    trait FromString[+E]:
        def apply(msg: String): E

    object FromString:
        given FromString[String] = a => a
        given convertible[A](using c: Conversion[String, A]): FromString[A] = a => c(a)

    inline def pass[E]: Validated[E, Unit] = Valid(())

    extension [E](e: E)
        def err[A]: Validated[E, A] = Invalid(e)

    extension [A](a: A)
        def asValid[E]: Validated[E, A] = Valid(a)

    extension [E](es: Seq[E])
        def asErrors[A](ifEmpty: A): Validated[E, A] = es match
            case h +: t => Invalid(h, t)
            case _ => Valid(ifEmpty)

    extension [A](a: Option[A])
        def toValid[E](err: => E): Validated[E, A] = a.fold(Invalid(err))(Valid.apply)

    given monadInstances[E]: Monad[[x] =>> Validated[E, x]] with  {

      extension [A](a: A) override def pure: Validated[E, A] = Valid(a)

      extension [A](ma: Validated[E, A])

        override def map[B](f: A => B): Validated[E, B] = 
            ma match
                case Valid(a) => Valid(f(a))
                case other => other.asInstanceOf[Validated[E, B]]

        override def ap[B](mf: Validated[E, A => B]): Validated[E, B] =
            (ma, mf) match 
                case (Invalid(hl, tl), Invalid(hr, tr)) => Invalid(hl, (tl :+ hr) ++ tr)  
                case (Valid(_), inv: Invalid[_, _]) => inv.asInstanceOf[Validated[E, B]]  
                case (inv: Invalid[_, _], Valid(_)) => inv.asInstanceOf[Validated[E, B]]
                case (Valid(v), Valid(f)) => Valid(f(v))

      extension [A](ma: Validated[E, A]) 
        override def flatMap[B](f: A => Validated[E, B]): Validated[E, B] = 
            ma match
                case Valid(a) => f(a)
                case other => other.asInstanceOf[Validated[E, B]]
    }

    extension [E, A](es: Seq[Validated[E, A]])
        def sequence: Validated[E, Seq[A]] =
            es.foldLeft[Validated[E, Seq[A]]](Valid(Nil)) { (c, i) =>
                c.zip(i).map {case l ~ r => r +: l}
            }.map(_.reverse)