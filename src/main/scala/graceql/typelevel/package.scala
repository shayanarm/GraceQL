package graceql

package object typelevel:
  type FullyApplied[T] = T match
    case (a1 => b)     => FullyApplied[b]
    case (a1, a2 => b) => FullyApplied[b]
    case (a1, a2, a3 => b) => FullyApplied[b]
    case (a1, a2, a3, a4 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21 => b) => FullyApplied[b]
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22 => b) => FullyApplied[b]
    case _             => T

  type R[C[_],X] = X match
    case (a1 => b) => (a1 => R[C,b])
    case (a1, a2 => b) => (a1, a2 => R[C,b])
    case (a1, a2, a3 => b) => (a1, a2, a3 => R[C,b])
    case (a1, a2, a3, a4 => b) => (a1, a2, a3, a4 => R[C,b])
    case (a1, a2, a3, a4, a5 => b) => (a1, a2, a3, a4, a5 => R[C,b])
    case (a1, a2, a3, a4, a5, a6 => b) => (a1, a2, a3, a4, a5, a6 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7 => b) => (a1, a2, a3, a4, a5, a6, a7 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8 => b) => (a1, a2, a3, a4, a5, a6, a7, a8 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21 => R[C,b])
    case (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22 => b) => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22 => R[C,b])
    case _ => C[X]