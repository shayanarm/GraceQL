package graceql.data

case class Kleisli[M[_]: Monad, A, B](run: A => M[B]):
    def compose[C](k: Kleisli[M, C, A]): Kleisli[M, C, B] =
        Kleisli(c => k.run(c).flatMap(this.run))    

    inline def andThen[C](k: Kleisli[M, B, C]): Kleisli[M, A, C] = 
        k `compose` this    
    
    inline def <#[C](k: Kleisli[M, C, A]): Kleisli[M, C, B] = 
        this `compose` k

    inline def #>[C](k: Kleisli[M, B, C]): Kleisli[M, A, C] = 
        k <# this

object Kleisli:
    given convFunc[M[_] : Monad, A, B]: Conversion[A => M[B], Kleisli[M, A, B]] = Kleisli.apply

    given km[M[_], A](using m: Monad[M]): Monad[[x] =>> Kleisli[M, A, x]] with {

        extension [B](b: B)
            def pure: Kleisli[M, A, B] = Kleisli(_ => m.pure(b))

        extension [B](ma: Kleisli[M, A, B])
            def flatMap[C](f: B => Kleisli[M, A, C]): Kleisli[M, A, C] = 
                Kleisli(a => m.flatMap(ma.run(a))(b => f(b).run(a)))
    }