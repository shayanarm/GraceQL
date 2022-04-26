package graceql.data

enum Source[+R[_],+V[_], A](underlying: R[A] | V[A]):
    case Ref[+R[_], A](ref: R[A]) extends Source[R,Nothing,A](ref)
    case Values[+V[_], A](values: V[A]) extends Source[Nothing,V,A](values)