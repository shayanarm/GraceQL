package graceql

package object syntax {
  extension[A](a: => A)  
    inline def |>[B](f: A => B): B = f(a)
}
