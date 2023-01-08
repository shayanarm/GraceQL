package graceql.data


class State[S, A](val runState: S => (A, S)):
    def run(s: S): A = runState(s)._1

object State:
  given monad[S]: Monad[[x] =>> State[S, x]] with

    extension [A](a: A) override def pure: State[S, A] = State(s => (a, s))

    extension [A](sa: State[S, A])
      override def map[B](f: A => B): State[S, B] =
        State { s =>
          sa.runState(s) match
            case (a, s2) => (f(a), s2)
        }
    
      override def ap[B](mf: State[S, A => B]): State[S, B] =
        State { s =>
           sa.runState(s) match
            case (a, s2) => mf.runState(s2) match
                case (f, s3) => (f(a), s3)
        }

      override def flatMap[B](f: A => State[S, B]): State[S, B] =
        State { s => 
            sa.runState(s) match
                case (a, s2) => f(a).runState(s2)    
        }
