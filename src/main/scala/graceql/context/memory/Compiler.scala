package graceql.context.memory

import scala.quoted.*
import graceql.core.*
import graceql.quoted.Compiled

object Compiler {
  def compile[R[_], M[_], A](e: Expr[A])(using
      q: Quotes,
      tr: Type[R],
      tm: Type[M],
      ta: Type[A]
  ): Expr[Compiled[() => A]] =
    import q.reflect.*
    Compiled.`catch` {
      new TreeTraverser {
        override def traverseTree(tree: Tree)(owner: Symbol): Unit =
          tree match
            case Select(i, call)
                if i.tpe <:< TypeRepr.of[Queryable[R, M, [x] =>> () => x]] =>
              call match
                case "create" =>
                  throw GraceException(
                    "`in-memory` context refs cannot be created"
                  )
                case "delete" =>
                  throw GraceException(
                    "`in-memory` context refs cannot be deleted"
                  )
                case "native" =>
                  throw GraceException(
                    "`in-memory` contexts do not support native syntax"
                  )
                case "typed" =>
                  throw GraceException(
                    "`in-memory` contexts do not support native syntax"
                  )
                case _ => super.traverseTree(tree)(owner)
            case _ => super.traverseTree(tree)(owner)
      }.traverseTree(e.asTerm)(Symbol.spliceOwner)

      '{ () => $e }
    }
}
