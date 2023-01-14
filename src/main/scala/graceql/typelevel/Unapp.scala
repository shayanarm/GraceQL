package graceql.typelevel

trait Unapp[T, A, M[_]](val ev: T =:= M[A]) {
  final type F[X] = M[X]
  final def unapp(t: T): M[A] = ev(t)
  final def app(t: M[A]): T = ev.flip(t)
}

object Unapp extends UnappPriority4:
  type Of[T, A] = Unapp[T, A, _]

  def apply[T, A, M[_]](ev: T =:= M[A]) = new Unapp[T, A, M](ev) {}
  given id[A]: Unapp[A, A, [x] =>> x] = Unapp(summon[A =:= A])

trait UnappPriority0:

  given const[T, A]: Unapp[T, A, [x] =>> T] = Unapp(summon[T =:= T])

trait UnappPriority1 extends UnappPriority0:
  given deep1[O[_], A, I, G[_]](using
      inner: Unapp[I, A, G],
      ev: O[I] =:= O[G[A]]
  ): Unapp[O[I], A, [x] =>> O[G[x]]] = Unapp(ev)

trait UnappPriority2 extends UnappPriority1:
  given deep2[O[_, _], A, I1, I2, G1[_], G2[_]](using
      i1: Unapp[I1, A, G1],
      i2: Unapp[I2, A, G2],
      ev: O[I1, I2] =:= O[G1[A], G2[A]]
  ): Unapp[O[I1, I2], A, [x] =>> O[G1[x], G2[x]]] = Unapp(ev)

trait UnappPriority3 extends UnappPriority2:
  given deep3[O[_, _, _], A, I1, I2, I3, G1[_], G2[_], G3[_]](using
      i1: Unapp[I1, A, G1],
      i2: Unapp[I2, A, G2],
      i3: Unapp[I3, A, G3],
      ev: O[I1, I2, I3] =:= O[G1[A], G2[A], G3[A]]
  ): Unapp[O[I1, I2, I3], A, [x] =>> O[G1[x], G2[x], G3[x]]] = Unapp(ev)  

trait UnappPriority4 extends UnappPriority3:
  given deep4[O[_, _, _, _], A, I1, I2, I3, I4, G1[_], G2[_], G3[_], G4[_]](
      using
      i1: Unapp[I1, A, G1],
      i2: Unapp[I2, A, G2],
      i3: Unapp[I3, A, G3],
      i4: Unapp[I4, A, G4],
      ev: O[I1, I2, I3, I4] =:= O[G1[A], G2[A], G3[A], G4[A]]
  ): Unapp[O[I1, I2, I3, I4], A, [x] =>> O[G1[x], G2[x], G3[x], G4[x]]] =
   Unapp(ev)
