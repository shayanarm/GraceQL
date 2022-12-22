package graceql.data

enum Ior[+L, +R]:
  case Left[L](value: L) extends Ior[L, Nothing]
  case Right[R](value: R) extends Ior[Nothing, R]
  case Both[L, R](left: L, right: R) extends Ior[L, R]

  
object Ior:
  given monad[L]: Monad[[x] =>> Ior[L, x]] with
    extension [R](r: R) 
      def pure: Ior[L, R] = Right(r)
      
    extension [R1](ma: Ior[L, R1]) 
      def flatMap[R2](f: R1 => Ior[L, R2]): Ior[L, R2] = 
        ma match
          case l: Left[_] => l.asInstanceOf[Ior[L, R2]]
          case Right(r) => f(r)
          case Both(l,r) => f(r)
    
