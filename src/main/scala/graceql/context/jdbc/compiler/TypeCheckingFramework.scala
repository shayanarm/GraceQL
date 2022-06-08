package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps

trait TypeCheckingFramework(using val q: Quotes) {
  self =>
  import q.reflect.*

  extension [L, R](es: List[Either[L, R]])
    def sequence: Either[List[L], List[R]] =
      es.foldLeft[Either[List[L], List[R]]](Right(Nil)) {
        case (Right(ms), Right(m)) => Right(m :: ms)
        case (Left(es), Left(e))   => Left(e :: es)
        case (l @ Left(es), _)     => l
        case (_, Left(e))          => Left(List(e))
      }.map(_.reverse)

  extension (tpe: Type[_])
    def no: Type[_] =
      tpe match
        case '[Option[a]] => Type.of[a]
        case _            => tpe
  extension (trep: TypeRepr)
    def no: TypeRepr =
      trep.asType match
        case '[a] => TypeRepr.of(using Type.of[a].no)

  case class ValidatedSchema[A](
      name: String,
      compositeUniques: List[String],
      fieldSpecs: List[FieldSpec[_]]
  ):
    def forAST: List[CreateSpec[Expr, Type]] =
      val renamedUniques = compositeUniques
        .flatMap { n =>
          fieldSpecs.collect {
            case fs if fs.name == n => fs.resolvedName
          }
        }
      (FieldSpec.forAST(fieldSpecs), renamedUniques) match
        case (l, Nil) => l
        case (l, r)   => l :+ CreateSpec.Uniques[Expr, Type](r)

  case class FieldSpec[A](
      name: String,
      tpe: Type[A],
      mods: List[modifier],
      default: Option[Expr[A]]
  ):
    def resolvedName = nameOverride.getOrElse(name).trim
    def nameOverride = mods.collectFirst { case ann: name =>
      ann.value
    }

  object FieldSpec:
    def forType[A](using
        Type[A]
    ): Either[List[String], List[FieldSpec[_ <: Any]]] =
      val trep = TypeRepr.of[A]
      val caseFields = trep.typeSymbol.caseFields
      val companion = trep.typeSymbol.companionClass
      val fields: List[Either[List[String], FieldSpec[_ <: Any]]] =
        caseFields.zipWithIndex.map((s, i) =>
          val fieldType = trep.memberType(s)
          fieldType.asType match
            case '[t] =>
              val default = companion
                .declaredMethod(s"$$lessinit$$greater$$default$$${i + 1}")
                .headOption
                .map(Select(Ref(trep.typeSymbol.companionModule), _))
                .map(_.asExprOf[t])
              List(
                annotationFor[name](s),
                annotationFor[pk](s),
                annotationFor[fk](s),
                annotationFor[autoinc](s),
                annotationFor[unique](s),
                annotationFor[index](s)
              ).sequence
                .map(_.flatten)
                .map { mods =>
                  FieldSpec[t](s.name, Type.of[t], mods, default)
                }
        )
      fields.sequence.left.map(_.flatten)

    def forAST(specs: List[FieldSpec[_]]): List[CreateSpec[Expr, Type]] =
      val colDefs = specs.map { case fs @ FieldSpec(_, tpe, mods, default) =>
        val astDefault = default.map(ColMod.Default(_))
        tpe match
          case '[Option[a]] =>
            CreateSpec.ColDef[Expr, Type, a](
              fs.resolvedName,
              Type.of[a],
              for d <- mods.collect {
                  case _: autoinc => ColMod.AutoInc[Expr]()
                  case _: unique  => ColMod.Unique[Expr]()
                } ++ astDefault.toList
              yield d
            )
          case '[a] =>
            CreateSpec.ColDef[Expr, Type, a](
              fs.resolvedName,
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
            spec.resolvedName
          }.toList
        yield v) match
          case Nil => None
          case ks  => Some(CreateSpec.PK(ks))

      val indices: Option[CreateSpec[Expr, Type]] =
        (for
          spec <- specs
          v <- spec.mods.collectFirst { case index(ord) =>
            (spec.resolvedName, ord)
          }.toList
        yield v) match
          case Nil  => None
          case idxs => Some(CreateSpec.Index(idxs))

      val uniques: Option[CreateSpec[Expr, Type]] =
        (for
          spec <- specs
          v <- spec.mods.collectFirst { case _: unique =>
            spec.resolvedName
          }.toList
        yield v) match
          case Nil => None
          case ks  => Some(CreateSpec.Uniques(ks))
      val fks: List[CreateSpec[Expr, Type]] = (for
        spec <- specs
        v <- spec.mods.collectFirst { case fk(tpe: Type[_], ref, onDel) =>
          tpe match
            case '[a] =>
              CreateSpec.FK[Expr, Type](
                spec.resolvedName,
                Expr(require.tableName[a]),
                ref,
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
      ann <- annotationFor[schema](TypeRepr.of(using ta).typeSymbol)
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

  def validateSchema[A](using Type[A]): Either[String, ValidatedSchema[A]] =
    val r = for
      mirr <- Expr
        .summon[SQLEncoding.Of[A]]
        .toRight[List[String]](List(errorStrings.sqlEncodingNotFound[A]))
      trep: TypeRepr <- mirr match
        case '{ $row: SQLRow[a] } => Right(TypeRepr.of[a])
        case _ =>
          Left(
            List(
              s"Target type must derive ${TypeRepr.of[SQLRow].typeSymbol.name} to be qualified as a table."
            )
          )
      sch <- tableSchema[A].left.map(List(_))
      schema(tname, uniques*) = sch
      fieldSpecs <- FieldSpec.forType[A]
      _ <- fieldSpecs match
        case Nil =>
          Left(
            List("Target type must be a `case class` with atleast one field.")
          )
        case fields => Right(fields)
      _ <-
        val columnTypeErr = fieldSpecs.exists { case FieldSpec(_, tr, _, d) =>
          tr match
            case '[a] =>
              require.instance[SQLEncoding.Of[a]] match
                case '{ $row: SQLRow[a] } => true
                case _                    => false
        } match
          case true =>
            Some(
              s"Columns of the table type cannot derive from ${TypeRepr.of[SQLRow].typeSymbol.name}."
            )
          case _ => None
        val fieldNameErrors = fieldSpecs.flatMap { fs =>
          if fs.resolvedName.isBlank then
            List(
              s"Resolved SQL column name for the case field `${fs.name}` cannot be blank"
            )
          else Nil
        }
        val modErrs: List[String] =
          for
            fs <- fieldSpecs
            mod <- fs.mods
            err <- {
              mod match
                case fk(tpe: Type[_], field, onDel) =>
                  val onDelErrors = (fs.tpe, onDel) match
                    case ('[Option[a]], OnDelete.SetNull) => Nil
                    case (_, OnDelete.SetNull) =>
                      List(
                        "`OnDelete.SetNull` can only be specified for nullable columns"
                      )
                    case _ => Nil
                  val fkErrors = tpe match
                    case '[a] =>
                      TypeRepr
                        .of[a]
                        .typeSymbol
                        .caseFields
                        .find(_.name == field) match
                        case Some(symb) =>
                          TypeRepr.of[a].memberType(symb) match
                            case reftrep
                                if reftrep.no == TypeRepr.of(using fs.tpe).no =>
                              Nil
                            case reftrep =>
                              List(
                                s"The Non-Option type of foreign key field `${fs.name}: ${Type
                                    .show(using fs.tpe.no)}` does not match that " +
                                  s"of its referenced field `${field}: ${Type
                                      .show(using reftrep.no.asType)}` of the " +
                                  s"case class `${Type.show(using tpe)}`"
                              )
                        case None =>
                          List(
                            s"Referenced foreign key column `$field` on class ${Type.show[a]} does not exist"
                          )
                  onDelErrors ++ fkErrors
                case _ => Nil
            }
          yield err

        columnTypeErr.toList ++ fieldNameErrors ++ modErrs match
          case Nil  => Right(())
          case errs => Left(errs)
    yield ValidatedSchema[A](tname, uniques.toList, fieldSpecs)
    r.left.map(
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
      FieldSpec.forType[T] match
        case Right(specs) => specs
        case Left(errs) =>
          throw GraceException(
            errs
              .map(e => s"-  $e")
              .mkString(
                s"Error fetching field information for type ${Type.show[T]}:\n",
                "\n",
                ""
              )
          )

  object errorStrings:
    def sqlEncodingNotFound[A](using Type[A]): String =
      s"No instance of ${Type.show[SQLEncoding.Of[A]]} found for type ${Type.show[A]}"

}
