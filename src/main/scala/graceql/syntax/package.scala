package graceql

import graceql.data.~

package object syntax {
  extension[A](a: A)  
    inline def |>[B](f: A => B): B = f(a)
    inline def ~[B](b: B): A ~ B = (a, b)
}
