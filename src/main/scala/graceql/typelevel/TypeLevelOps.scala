package graceql.typelevel

type If[C <: Boolean, K, T <: K, F <: K] <: K = C match
  case true => T
  case false => F