package graceql

import graceql.data.*

package object syntax {
  extension[A](a: A)  
    inline def |>[B](f: A => B): B = f(a)

    inline def pure[M[_]](using app: Applicative[M]): M[A] = app.pure(a)
    
    inline def lift[M[_]](using app: Applicative[M]): M[A] = a.pure

  inline def pass[M[_]](using app: Applicative[M]): M[Unit] = ().pure

  extension [F[_]: Traverse, A](fa: F[A])
    inline def traverse[G[_]: Applicative, B](f: A => G[B]): G[F[B]] = summon[Traverse[F]].traverse(f)(fa)
  
  extension [F[_]: Traverse, G[_]: Applicative, A](fa: F[G[A]])
    inline def sequence: G[F[A]] = summon[Traverse[F]].sequence(fa)
}
