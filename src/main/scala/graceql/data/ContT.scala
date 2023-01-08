package graceql.data

type Cont[R, A] = ContT[R, Id, A]

trait ContT[R, M[_], A]:
  def run(f: A => M[R]): M[R]
  inline def apply(f: A ?=> M[R]): M[R] = run(a => f(using a))  

object ContT:
    def apply[R, M[_], A](f: (A => M[R]) => M[R]): ContT[R, M, A] = g => f(g)

    given monad[R, M[_]]: Monad[[x] =>> ContT[R, M, x]] with

      extension [A](a: A) 
        override def pure: ContT[R, M, A] = ContT(f => f(a))

      extension [A](ca: ContT[R, M, A])
        override def map[B](f: A => B): ContT[R, M, B] =
            ContT(g => ca.run(g compose f))
        override def flatMap[B](f: A => ContT[R, M, B]): ContT[R, M, B] = 
            ContT(g => ca.run(a => f(a).run(g)))