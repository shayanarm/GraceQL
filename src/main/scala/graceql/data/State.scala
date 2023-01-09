package graceql.data

import graceql.syntax.*

class State[S, A](val run: S => (A, S)):
  def eval(s: S): A = run(s)._1
  def exec(s: S): S = run(s)._2

object State:

  def get[S]: State[S, S] = State(s => (s, s))

  def put[S](s: S): State[S, Unit] = State(_ => ((), s))

  def update[S](f: S => S): State[S, Unit] = State(s => ((), f(s)))

  given monad[S]: Monad[[x] =>> State[S, x]] with

    extension [A](a: A) override def pure: State[S, A] = State(s => (a, s))

    extension [A](sa: State[S, A])
      override def map[B](f: A => B): State[S, B] =
        State { s =>
          sa.run(s) match
            case (a, s2) => (f(a), s2)
        }

      override def ap[B](mf: State[S, A => B]): State[S, B] =
        State { s =>
          sa.run(s) match
            case (a, s2) =>
              mf.run(s2) match
                case (f, s3) => (f(a), s3)
        }

      override def flatMap[B](f: A => State[S, B]): State[S, B] =
        State { s =>
          sa.run(s) match
            case (a, s2) => f(a).run(s2)
        }
