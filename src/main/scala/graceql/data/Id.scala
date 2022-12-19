package graceql.data

opaque type Id[+A] = A

object Id:
    inline def apply[A](a: A): Id[A] = a

    extension [A](a: Id[A])
        inline def unwrap: A = a

    given Monad[Id] with {

      extension [A](a: A) override inline def pure: Id[A] = a

      extension [A](ma: Id[A]) override inline def flatMap[B](f: A => Id[B]): Id[B] = f(ma)
    }
