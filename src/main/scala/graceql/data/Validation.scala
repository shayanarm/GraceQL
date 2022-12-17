package graceql.data

import scala.collection.WithFilter
import java.util.NoSuchElementException

case class ~[+A, +B](l: A, r: B)

enum Validation[E, +A]:
    case Valid[E, A](value: A) extends Validation[E, A]
    case Invalid[E, A] (errors: Seq[E]) extends Validation[E, A]

    def getOrElse[B >: A](a: => B): B = this match
        case Valid(v) => v
        case _ => a
    
    def get(): A = getOrElse(throw NoSuchElementException())

    def zip[B](other: Validation[E, B]): Validation[E, A ~ B] =
        this.ap(other.map(b => a => graceql.data.~(a, b)))    

    inline def ~[B](other: Validation[E, B]): Validation[E, A ~ B] = zip(other)

    inline def ^^[B](f: A => B) = this.map(f)

    def mapError[O](f: E => O): Validation[O, A] =
        this match
            case Invalid(errors) => Invalid(errors.map(f))
            case other => other.asInstanceOf[Validation[O, A]] 

    def filter(err: => E)(pred: A => Boolean): Validation[E, A] =
        this match
            case v@ Valid(a) if !pred(a) => Invalid(Seq(err))
            case other => other
    
    def filter(pred: A => Boolean)(using conv: Validation.FromString[E]): Validation[E, A] =
        filter(conv("Predicate for `Validation.filter` failed"))(pred)

    def withFilter(pred: A => Boolean)(using conv: Validation.FromString[E]): Validation[E, A] =
        filter(pred)        
        

object Validation:
    import Validation.*;

    trait FromString[+E]:
        def apply(msg: String): E

    object FromString:
        given FromString[String] = a => a
        given convertible[A](using c: Conversion[String, A]): FromString[A] = a => c(a)

    extension [E](e: E)
        def err[A]: Validation[E, A] = Invalid(Seq(e))

    given monadInstances[E]: Monad[[x] =>> Validation[E, x]] with  {

      extension [A](a: A) override def pure: Validation[E, A] = Valid(a)

      extension [A](ma: Validation[E, A]) override def ap[B](mf: Validation[E, A => B]): Validation[E, B] =
        (ma, mf) match 
            case (Invalid(l), Invalid(r)) => Invalid(l ++ r)  
            case (Valid(_), Invalid(e)) => Invalid(e)  
            case (Invalid(e), Valid(_)) => Invalid(e)
            case (Valid(v), Valid(f)) => Valid(f(v))

      extension [A](ma: Validation[E, A]) override def flatMap[B](f: A => Validation[E, B]): Validation[E, B] = 
        ma match
            case Valid(a) => f(a)
            case other => other.asInstanceOf[Validation[E, B]]
    }

    extension [E, A](es: Seq[Validation[E, A]])
        def sequence: Validation[E, Seq[A]] =
            es.foldLeft[Validation[E, Seq[A]]](Valid(Nil)) { (c, i) =>
                c.zip(i).map {case l ~ r => r +: l}
            }.map(_.reverse)