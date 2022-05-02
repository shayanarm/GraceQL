import scala.quoted.*
import graceql.core.*
import graceql.data.MonadError
import scala.compiletime.summonInline

package object graceql {
  export graceql.core.Capabilities
  export graceql.core.QueryContext
  export graceql.core.SchemaContext
  export graceql.core.Transaction
  export graceql.core.Transaction.transaction

  transparent inline def query[R[_], M[+_]]: context.CallProxy[[x[_]] =>> QueryContext[x, M], R] = 
    context[[x[_]] =>> QueryContext[x, M], R]

  transparent inline def schema[R[_]]: context.CallProxy[SchemaContext, R] = 
    context[SchemaContext, R]

  transparent inline def dml[R[_], M[+_]] = query[R, M]  
  transparent inline def ddl[R[_]] = schema[R]

  class Functions[C[+_]]:

    inline def nullary[A](using c: Capabilities[C])(inline f: C[A]): () => A = 
      () => c.fromNative(f)
    inline def unary[A, B](using c: Capabilities[C])(inline f: C[A] => C[B]): A => B = 
      a => c.fromNative(f(c.toNative(a)))
    inline def binary[A1, A2, B](using c: Capabilities[C])(inline f: (C[A1], C[A2]) => C[B]): (A1, A2) => B = 
      (a1, a2) => c.fromNative(f(c.toNative(a1), c.toNative(a2)))
    inline def ternary[A1, A2, A3, B](using c: Capabilities[C])(inline f: (C[A1], C[A2], C[A3]) => C[B]): (A1, A2, A3) => B =
      (a1, a2, a3) => c.fromNative(f(c.toNative(a1), c.toNative(a2), c.toNative(a3)))
    inline def quarternary[A1, A2, A3, A4, B](using c: Capabilities[C])(inline f: (C[A1], C[A2], C[A3], C[A4]) => C[B]): (A1, A2, A3, A4) => B =
      (a1, a2, a3, a4) => c.fromNative(f(c.toNative(a1), c.toNative(a2), c.toNative(a3), c.toNative(a4)))      

  inline def function[C[+_]](using c: Capabilities[C]) = Functions[C]()
  inline def fromNative[A, C[+_]](using c: Capabilities[C])(bin: C[A]) = c.fromNative(bin)
}
