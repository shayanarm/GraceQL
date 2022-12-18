package graceql.core

import scala.quoted.Quotes
import graceql.data.Validated
import scala.quoted.*
import graceql.data.Applicative.pure

trait CompilationFramework(using val q: Quotes) {
    import q.reflect.*
    import Validated.*

    final type Result[+A] = Validated[String, A]

    extension (tpe: Type[_])
        def no: Type[_] =
        tpe match
            case '[Option[a]] => Type.of[a]
            case _            => tpe
        def typeRepr: TypeRepr = TypeRepr.of(using tpe)

    extension (trep: TypeRepr)
        def no: TypeRepr =
        trep.asType match
            case '[a] => Type.of[a].no.typeRepr

    extension (errs: Seq[String])
        def listString(headline: String = "Errors encountered"): Option[String] =
        if errs.isEmpty then None
        else
            Some(
            errs
                .map(e => s"-  $e")
                .mkString(
                s"$headline:\n",
                "\n",
                ""
                )
            )
    type Requirements <: BaseRequirements
    trait BaseRequirements:
        def apply[E, A](v: Result[A])(msg: => String = "Requirement failed"): A =
        v match
            case Valid(v) => v
            case i: Invalid[_, _] => throw GraceException(i.errors.listString(msg).get)

        def instance[T](using Type[T]): Expr[T] =
        require(summonValid[T])("Type Class instance not found")

    val require: Requirements

    def withImplicit[P, A](p: Expr[P])(
        f: Expr[P ?=> A]
    )(using Type[P], Type[A]): Expr[A] =
        preprocess('{ $f(using $p) })

    def preprocess[A](
        e: Expr[A]
    )(using ta: Type[A]): Expr[A] =
        import graceql.quoted.CompileOps.*
        val pipe =
        inlineDefs andThen
            betaReduceAll andThen
            inlineDefs
        pipe(e.asTerm).asExprOf[A]

    def summonValid[T](using Type[T]): Result[Expr[T]] =
        Expr.summon[T].toValid(s"Failed to obtain an instances for ${Type.show[T]}")
}
