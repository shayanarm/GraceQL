package graceql.quoted

import scala.reflect.Typeable
import graceql.data.Eq
import scala.collection.mutable
import scala.quoted.*
import scala.PolyFunction
import scala.annotation.experimental

object Memoize:

  def memoize(f: Expr[Any])(using q: Quotes): Expr[Any] =
    import q.reflect.*
    f.asTerm.tpe match
      case tt@ Refinement(
            TypeRef(
              ThisType(
                TypeRef(
                  NoPrefix(),
                  "scala"
                )
              ),
              "PolyFunction"
            ),
            "apply",
            p@ PolyType(_, _, _)
          ) => memoizePoly(f)(p)
      case _ => memoizeConcrete(f)
  
  protected def memoizePoly(f: Expr[Any])(using q: Quotes)(pt: q.reflect.PolyType): Expr[Any] = 
    ???
    // import q.reflect.*

    // pt match
    //       case PolyType(
    //             typeParams,
    //             _,
    //             MethodType(
    //               _,
    //               pts,
    //               rt
    //             )
    //           ) => 
    //             val tupledArgs = pts.foldRight[Type[_]](Type.of[EmptyTuple]) { (h, t) =>
    //               Applied(
    //                 TypeTree.of[*:],
    //                 List(TypeTree.of(using h.asType), TypeTree.of(using t))
    //               ).tpe.asType
    //             }
    //             val fnType = rt.asType match
    //               case '[x] => (tupledArgs, memoType[x]) match
    //                 case ('[a], '[b]) => Type.of[a => b]
    //             (tupledArgs, fnType) match
    //               case ('[args], '[r]) =>
    //                   val name: String = "$anon"
    //                   val parents = List(TypeTree.of[PolyFunction])
    //                   def decls(cls: Symbol): List[Symbol] =
    //                     List(
    //                       Symbol.newMethod(cls, "apply", 
    //                         MethodType(List("ta", "ea"))({mt => 
    //                           List(
    //                             TypeRepr.of[Typeable[args]],
    //                             TypeRepr.of[Eq[args]]
    //                           )
    //                         }, mt => TypeRepr.of(using fnType))
    //                       )
    //                     )

    //                   val cls = Symbol.newClass(Symbol.spliceOwner, name, parents = parents.map(_.tpe), decls, selfType = None)
    //                   val applySym = cls.declaredMethod("apply").head

    //                   val applyDef = DefDef(applySym, argss => Some('{println(s"Calling apply")}.asTerm))
    //                   val clsDef = ClassDef(cls, parents, body = List(applyDef))
    //                   val newCls = Typed(Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil), TypeTree.of[PolyFunction])

    //                   Block(List(clsDef), newCls).asExpr

  protected def memoizeConcrete(f: Expr[Any])(using q: Quotes): Expr[Any] =
    import q.reflect.*
    val tupled = f match
      case '{$g: (a => b)} => g
      case '{$g: ((a1, a2) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21) => b)} => '{$g.tupled}
      case '{$g: ((a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22) => b)} => '{$g.tupled}
      case _ => f

      tupled match
        case '{$g: (a => b)} => 
          memoize1(g)
        case other => other

  protected def memoType[A](using q: Quotes, tp: Type[A]) = 
    import q.reflect.*
    val dummy: Expr[A] = '{CompileOps.placeholder[A]}
    memoize(dummy).asTerm.tpe.asType      
  protected def memoize1(f: Expr[Any])(using q: Quotes): Expr[Any] =
    import q.reflect.*
    f match
      case '{$g: (a => b)} =>
        memoType[b] match
          case '[c] =>
            '{
              (tt: Typeable[a], ev: Eq[a]) ?=> new Function[a, c] { self =>
                val memo: mutable.ArrayBuffer[(Any, Any)] = mutable.ArrayBuffer()
                def apply(arg: a): c =
                  self.synchronized {
                    memo
                      .collectFirst {
                        case (tt(i), o) if ev.eqv(i, arg) => o
                      }
                      .fold {
                        val r = ${memoize('{$g(arg)}).asExprOf[c]}
                        memo.append((arg.asInstanceOf[Any], r))
                        r
                      }(o => o.asInstanceOf[c])
                  }         
              }
            }