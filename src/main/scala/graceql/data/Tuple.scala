package graceql.data

type ~[+A, +B] = (A, B)

object ~ :
    def unapply[A,B](v: A ~ B): Option[(A, B)] = Some((v._1, v._2))
    // def apply[A,B](a: A, b: B): A ~ B = (a, b)
