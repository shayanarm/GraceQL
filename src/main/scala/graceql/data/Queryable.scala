package graceql.data

trait Functor[M[_]]:
  extension [A](ma: M[A]) def map[B](f: A => B): M[B]

trait Applicative[M[_]] extends Functor[M]:
  extension [A](a: A) @`inline` def pure: M[A]

  extension [A](ma: M[A])

    def ap[B](f: M[A => B]): M[B]

    inline def <*>[B](f: M[A => B]): M[B] = ap(f)
    override def map[B](f: A => B): M[B] =
      ma <*> f.pure

trait Monad[M[_]] extends Applicative[M]:
  extension [A](ma: M[A])

    def flatMap[B](f: A => M[B]): M[B]

    inline def >>=[B](f: A => M[B]): M[B] = flatMap(f)

    inline def bind[B](f: A => M[B]): M[B] = flatMap(f)

    override def ap[B](mf: M[A => B]): M[B] = mf.flatMap(ma.map)

trait MonadPlus[M[_]] extends Monad[M]:
  extension [A](ma: M[A])
    def concat(other: M[A]): M[A]
    inline def union(other: M[A]): M[A] = concat(other)
    inline def ++(other: M[A]): M[A] = concat(other)

trait MonadZero[M[_]] extends Monad[M]:
  extension [A](ma: M[A])
    def filter(pred: A => Boolean): M[A]
    def withFilter(pred: A => Boolean): scala.collection.WithFilter[A,M]

trait Queryable[M[_]] extends MonadZero[M] with MonadPlus[M]:
  extension [A](ma: M[A])
    def leftJoin[B](mb: M[B], on: (A, B) => Boolean): M[(A, Option[B])]
    def rightJoin[B](mb: M[B], on: (A, B) => Boolean): M[(Option[A], B)] =
      mb.leftJoin(ma, (b, a) => on(a, b)).map(p => p.swap)
    def crossJoin[B](mb: M[B], on: (A, B) => Boolean): M[(A, B)] =
      for
        a <- ma
        b <- mb if on(a, b)
      yield (a, b)
    def fullJoin[B](mb: M[B], on: (A, B) => Boolean): M[Ior[A, B]]
    def distinct: M[A]
    def groupBy[K](f: A => K): M[(K, M[A])]
    def groupMap[K,B](f: A => K)(mapper: (K, M[A]) => B): M[B] = groupBy(f).map(mapper.tupled)