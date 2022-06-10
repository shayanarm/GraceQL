package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps
import graceql.syntax.*

trait CompilationFramework(using val q: Quotes) {
  self =>
  import q.reflect.*

  extension (eith: Either.type)
    def sequence[L, R](es: List[Either[L, R]]): Either[List[L], List[R]] =
      es.foldLeft[Either[List[L], List[R]]](Right(Nil)) {
        case (Right(ms), Right(m)) => Right(m :: ms)
        case (Left(es), Left(e))   => Left(e :: es)
        case (l @ Left(es), _)     => l
        case (_, Left(e))          => Left(List(e))
      }.map(_.reverse)

  extension (strings: Iterable[String])
    def asErrors: Either[List[String], Unit] =
      if strings.size == 0 then Right(()) else Left(strings.toList)

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

  case class ValidatedSchema[A](
      name: String,
      compositeUniqueKey: List[String],
      fieldSpecs: List[FieldSpec[_]]
  ):
    def forAST: List[CreateSpec[Expr, Type]] =
      val renamedUniques = compositeUniqueKey
        .flatMap { n =>
          fieldSpecs.collect {
            case fs if fs.name == n => fs.sqlName
          }
        }
      (FieldSpec.forAST(fieldSpecs), renamedUniques) match
        case (l, Nil) => l
        case (l, r)   => l :+ CreateSpec.Uniques[Expr, Type](r)

  object ValidatedSchema:
    def forType[A](using Type[A]): Either[List[String], ValidatedSchema[A]] =
      for
        enc <- sqlEncoding[A].left.map(List(_))
        trep: TypeRepr <- enc match
          case '{ $row: SQLRow[a] } => Right(TypeRepr.of[a])
          case _ =>
            Left(
              List(
                s"Target type must derive ${TypeRepr.of[SQLRow].typeSymbol.name} to be qualified as a table."
              )
            )
        sch <- tableSchema[A].left.map(List(_))
        schema(tname, uniques*) = sch
        _ <- Either.cond(
          !tname.isBlank,
          (),
          List("Schema name cannot be blank")
        )
        fieldSpecs <- FieldSpec.allOf[A]
        compUnique <- uniques.toList.distinct match
          case Nil => Right(Nil)
          case us =>
            us.collect {
              case n if !fieldSpecs.exists(fs => fs.name == n) =>
                s"Field `$n` defined in the composite unique constraint does not exist"
            }.asErrors
              .map(_ => us)
        _ <- fieldSpecs match
          case Nil =>
            Left(
              List(
                "The type designated as a table must be a `case class` with atleast one field."
              )
            )
          case _ => Right(())
      yield ValidatedSchema[A](tname, compUnique, fieldSpecs)

  case class FieldSpec[A](
      name: String,
      tpe: Type[A],
      mirrorType: Type[_],
      mods: List[modifier],
      default: Option[Expr[A]]
  ):
    lazy val sqlName = nameOverride.getOrElse(name).trim
    lazy val nameOverride = mods.collectFirst { case ann: name =>
      ann.value
    }

  object FieldSpec:
    def forField[A](name: String)(using
        Type[A]
    ): Either[List[String], FieldSpec[_ <: Any]] =
      val trep = TypeRepr.of[A]
      val caseFields = trep.typeSymbol.caseFields
      val companion = trep.typeSymbol.companionClass
      for
        si <- caseFields.zipWithIndex
          .find((cf, i) => cf.name == name)
          .toRight(List(s"Field $name does not exist in ${Type.show[A]}"))
        (s, i) = si
        fieldType = trep.memberType(s)
        fs <- fieldType.asType match
          case '[t] =>
            val default = companion
              .declaredMethod(s"$$lessinit$$greater$$default$$${i + 1}")
              .headOption
              .map(Select(Ref(trep.typeSymbol.companionModule), _))
              .map(_.asExprOf[t])
            for
              enc <- sqlEncoding[t].left.map(List(_))
              _ <- enc match
                case '{ $e: SQLRow[a] } =>
                  Left(
                    List(
                      s"Field ${s.name} cannot derive from ${TypeRepr.of[SQLRow].typeSymbol.name}."
                    )
                  )
                case _ => Right(())
              m <- sqlMirror[t].left.map(List(_))
              mtpe = m.asTerm.tpe.asType match
                case '[SQLMirror[t, x]] => Type.of[x]
              mods <- List(
                annotationFor[name](s),
                annotationFor[pk](s),
                annotationFor[fk](s),
                annotationFor[autoinc](s),
                annotationFor[unique](s),
                annotationFor[index](s)
              ) |> Either.sequence map (_.flatten)
            yield FieldSpec[t](s.name, Type.of[t], mtpe, mods, default)
        _ <- fs.sqlName.isBlank match
          case true =>
            Left(
              List(
                s"Provided SQL column name for the case field `${fs.name}` cannot be blank"
              )
            )
          case false => Right(())
        _ <- fs.mods
          .collect { case fk(tpe: Type[_], field, onDel) =>
            List(
              (fs.tpe, onDel) match
                case ('[Option[a]], OnDelete.SetNull) => None
                case (_, OnDelete.SetNull) =>
                  Some(
                    s"Field `${fs.name}` is not nullable; `OnDelete.SetNull` can only be specified for nullable columns"
                  )
                case _ => None
              ,
              tpe match
                case '[a] =>
                  TypeRepr
                    .of[a]
                    .typeSymbol
                    .caseFields
                    .find(_.name == field) match
                    case Some(symb) =>
                      TypeRepr.of[a].memberType(symb) match
                        case reftrep if reftrep.no =:= fs.tpe.typeRepr.no =>
                          None
                        case reftrep =>
                          Some(
                            s"The Non-Option type of foreign key field `${fs.name}: ${Type
                                .show(using fs.tpe.no)}` does not match that " +
                              s"of its referenced field `${field}: ${Type
                                  .show(using reftrep.no.asType)}` of the " +
                              s"case class `${Type.show(using tpe)}`"
                          )
                    case None =>
                      Some(
                        s"Referenced foreign key field `$field` on record ${Type.show[a]} does not exist"
                      )
            ).flatten
          }
          .flatten
          .asErrors
      yield fs

    def allOf[A](using
        Type[A]
    ): Either[List[String], List[FieldSpec[_ <: Any]]] =
      val es =
        TypeRepr.of[A].typeSymbol.caseFields.map(cf => forField[A](cf.name))
      for
        specs <- Either.sequence(es).left.map(_.flatten)
        _ <- specs
          .groupBy(_.sqlName)
          .collect {
            case (n, acc) if acc.length > 1 => s"Duplicate column name: `$n`"
          }
          .asErrors
      yield specs

    def forAST(specs: List[FieldSpec[_]]): List[CreateSpec[Expr, Type]] =
      val colDefs = specs.map {
        case fs @ FieldSpec(_, tpe, mtpe, mods, default) =>
          val astDefault = default.map(ColMod.Default(_))
          mtpe match
            case '[Option[a]] =>
              CreateSpec.ColDef[Expr, Type, a](
                fs.sqlName,
                Type.of[a],
                for d <- mods.collect {
                    case _: autoinc => ColMod.AutoInc[Expr]()
                    case _: unique  => ColMod.Unique[Expr]()
                  } ++ astDefault.toList
                yield d
              )
            case '[a] =>
              CreateSpec.ColDef[Expr, Type, a](
                fs.sqlName,
                Type.of[a],
                for d <- mods.collect {
                    case _: autoinc => ColMod.AutoInc[Expr]()
                    case _: unique  => ColMod.Unique[Expr]()
                  } ++ List(ColMod.NotNull[Expr]()) ++ astDefault.toList
                yield d
              )
      }
      val pk: Option[CreateSpec[Expr, Type]] =
        (for
          spec <- specs
          v <- spec.mods.collectFirst { case _: pk =>
            spec.sqlName
          }.toList
        yield v) match
          case Nil => None
          case ks  => Some(CreateSpec.PK(ks))

      val indices: Option[CreateSpec[Expr, Type]] =
        (for
          spec <- specs
          v <- spec.mods.collectFirst { case index(ord) =>
            (spec.sqlName, ord)
          }.toList
        yield v) match
          case Nil  => None
          case idxs => Some(CreateSpec.Index(idxs))

      val uniques: Option[CreateSpec[Expr, Type]] =
        (for
          spec <- specs
          v <- spec.mods.collectFirst { case _: unique =>
            spec.sqlName
          }.toList
        yield v) match
          case Nil => None
          case ks  => Some(CreateSpec.Uniques(ks))
      val fks: List[CreateSpec[Expr, Type]] = (for
        spec <- specs
        v <- spec.mods.collectFirst { case fk(tpe: Type[_], ref, onDel) =>
          tpe match
            case '[a] =>
              val fs = require.fieldSpec[a](ref)
              CreateSpec.FK[Expr, Type](
                spec.sqlName,
                Expr(require.tableName[a]),
                fs.sqlName,
                onDel
              )
        }.toList
      yield v)
      colDefs ++ pk.toList ++ fks ++ indices.toList

  def preprocess[A](
      e: Expr[A]
  )(using ta: Type[A]): Expr[A] =
    import CompileOps.*
    val pipe =
      inlineDefs andThen
        betaReduceAll andThen
        inlineDefs
    pipe(e.asTerm).asExprOf[A]

  def withImplicit[P, A](p: Expr[P])(
      f: Expr[P ?=> A]
  )(using Type[P], Type[A]): Expr[A] =
    preprocess('{ $f(using $p) })

  def tableSchema[A](using ta: Type[A]): Either[String, schema] =
    for
      ann <- annotationFor[schema](ta.typeRepr.typeSymbol)
      sch <- ann.toRight(
        s"Missing ${Type.show[schema]} annotation for type ${Type.show[A]}"
      )
    yield sch

  def tableName[A](using ta: Type[A]): Either[String, String] =
    tableSchema[A].map(_.name)

  def annotationFor[T <: scala.annotation.StaticAnnotation](
      symb: Symbol
  )(using Type[T], FromExpr[T]): Either[String, Option[T]] =
    for
      opt <- Right(symb.getAnnotation(TypeRepr.of[T].typeSymbol))
      v <- opt.fold(Right(None)) { term =>
        Expr
          .unapply(term.asExprOf[T])
          .toRight(
            s"Static annotation ${Type.show[T]} for field ${symb.toString} cannot be unlifted. " +
              "Annotation must be constructed using literal values"
          )
          .map(Some(_))
      }
    yield v

  def sqlEncoding[A](using Type[A]): Either[String, Expr[SQLEncoding.Of[A]]] =
    Expr.summon[SQLEncoding.Of[A]] match
      case Some(enc) => Right(enc)
      case None =>
        Left(
          s"Failed to obtain an instances of ${Type.show[SQLEncoding.Of[A]]}"
        )

  def sqlMirror[A](using Type[A]): Either[String, Expr[SQLMirror.Of[A]]] =
    Expr.summon[SQLMirror.Of[A]] match
      case Some(enc) => Right(enc)
      case None =>
        Left(s"Failed to obtain an instances of ${Type.show[SQLMirror.Of[A]]}")

  def validateSchema[A](using Type[A]): Either[String, ValidatedSchema[A]] =
    ValidatedSchema
      .forType[A]
      .left
      .map(
        _.map(e => s"-  $e").mkString(
          s"Schema validation failed for type ${Type.show[A]}:\n",
          "\n",
          ""
        )
      )

  object assertions:
    def validSchema[A](using Type[A]): Unit =
      require.validSchema[A]
      ()

  object require:
    def instance[T](using Type[T]): Expr[T] =
      Expr.summon[T] match
        case Some(i) => i
        case None =>
          throw GraceException(
            s"Could not obtain an instance for ${Type.show[T]}"
          )

    def validSchema[T](using Type[T]): ValidatedSchema[T] =
      self.validateSchema[T] match
        case Right(n)  => n
        case Left(err) => throw GraceException(err)

    def tableName[T](using Type[T]): String = tableSchema[T].name

    def tableSchema[T](using Type[T]): schema =
      self.tableSchema[T] match
        case Right(n)  => n
        case Left(err) => throw GraceException(err)

    def fieldSpecs[T](using Type[T]): List[FieldSpec[_]] =
      FieldSpec.allOf[T] match
        case Right(specs) => specs
        case Left(errs) =>
          throw GraceException(
            errs
              .map(e => s"-  $e")
              .mkString(
                s"Error obtaining field information for type ${Type.show[T]}:\n",
                "\n",
                ""
              )
          )

    def fieldSpec[T](name: String)(using Type[T]): FieldSpec[_] =
      FieldSpec.forField[T](name) match
        case Right(fs) => fs
        case Left(errs) =>
          throw GraceException(
            errs
              .map(e => s"-  $e")
              .mkString(
                s"Error obtaining field information for type ${Type.show[T]}:\n",
                "\n",
                ""
              )
          )
}
