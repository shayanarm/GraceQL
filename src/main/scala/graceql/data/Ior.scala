package graceql.data

enum Ior[+L, +R]:
  case Left[L](value: L) extends Ior[L, Nothing]
  case Right[R](value: R) extends Ior[Nothing, R]
  case Both[L, R](left: L, right: R) extends Ior[L, R]

  
  def map[R2](f: R => R2): Ior[L, R2] = this match
    case Ior.Left(_)    => this.asInstanceOf[Ior[L, R2]]
    case Ior.Right(r)   => Right(f(r))
    case Ior.Both(l, r) => Both(l, f(r))

  def flatMap[L1 >: L, R2](f: R => Ior[L1, R2]): Ior[L1, R2] = this match
    case Ior.Left(_)    => this.asInstanceOf[Ior[L1, R2]]
    case Ior.Right(r)   => f(r)
    case Ior.Both(l, r) => f(r)  
