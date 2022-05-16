import scala.quoted.*
import graceql.core.*

package object graceql {
  export graceql.core.Capabilities
  export graceql.core.QueryContext
  export graceql.core.Transaction
  export graceql.core.Transaction.transaction

  transparent inline def query[R[_], M[+_]]: context.CallProxy[[x[_]] =>> QueryContext[x, M], R] = 
    context[[x[_]] =>> QueryContext[x, M], R]

  class FunctionsProxy[N[+_]](using c: Capabilities[N]):

    inline def nullary[A](inline f: N[A]): () => A = 
      () => f.unlift
    inline def unary[A, B](inline f: N[A] => N[B]): A => B = 
      a => f(a.lift).unlift
    inline def binary[A1, A2, B](inline f: (N[A1], N[A2]) => N[B]): (A1, A2) => B = 
      (a1, a2) => f(a1.lift, a2.lift).unlift
    inline def ternary[A1, A2, A3, B](inline f: (N[A1], N[A2], N[A3]) => N[B]): (A1, A2, A3) => B =
      (a1, a2, a3) => f(a1.lift, a2.lift, a3.lift).unlift
    inline def quarternary[A1, A2, A3, A4, B](inline f: (N[A1], N[A2], N[A3], N[A4]) => N[B]): (A1, A2, A3, A4) => B =
      (a1, a2, a3, a4) => f(a1.lift, a2.lift, a3.lift, a4.lift).unlift

  inline def function[N[+_]](using c: Capabilities[N]) = FunctionsProxy[N]()    
}
