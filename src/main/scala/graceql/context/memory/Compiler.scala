package graceql.context.memory

import scala.quoted.*
import graceql.core.*

object Compiler {
  def compile[R[_],M[+_],A](e: Expr[A])(using q: Quotes, tr: Type[R], tm: Type[M], ta: Type[A]): Expr[() => A] = 
    import q.reflect.*
    new TreeTraverser {
      override def traverseTree(tree: Tree)(owner: Symbol): Unit = 
        tree match
          case Select(i, call) if i.tpe <:< TypeRepr.of[Queryable[R,M,[x] =>> () => x]] =>
            call match
              case "create" => report.errorAndAbort("`in-memory` context refs cannot be created", tree.pos)
              case "delete" => report.errorAndAbort("`in-memory` context refs cannot be deleted", tree.pos)
              case "native" => report.errorAndAbort("`in-memory` contexts do not support native syntax", tree.pos)
              case "typed" => report.errorAndAbort("`in-memory` contexts do not support native syntax", tree.pos)
              case _ => super.traverseTree(tree)(owner)
          case _ => super.traverseTree(tree)(owner)      
    }.traverseTree(e.asTerm)(Symbol.spliceOwner)

    '{() => $e}
}

