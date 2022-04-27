package graceql.data

trait SQLLike[M[_]] extends MonadZero[M] with MonadPlus[M]:
  extension [A](ma: M[A])

    def size: Int

    inline def length: Int = size

    def leftJoin[B](mb: M[B])(on: (A, B) => Boolean): M[(A, Option[B])]

    def rightJoin[B](mb: M[B])(on: (A, B) => Boolean): M[(Option[A], B)] =
      mb.leftJoin(ma)((b, a) => on(a, b)).map(p => p.swap)

    inline def crossJoin[B](mb: M[B])(on: (A, B) => Boolean): M[(A, B)] =
      for
        a <- ma
        b <- mb if on(a, b)
      yield (a, b)

    def fullJoin[B](mb: M[B])(on: (A, B) => Boolean): M[Ior[A, B]]

    def distinct: M[A]

    def groupBy[K](f: A => K): M[(K, M[A])]

    inline def groupMap[K,B](f: A => K)(mapper: (K, M[A]) => B): M[B] = groupBy(f).map(mapper.tupled)