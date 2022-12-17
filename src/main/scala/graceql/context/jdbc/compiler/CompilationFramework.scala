package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps
import graceql.syntax.*
import graceql.data.{Validated, ~}

trait CompilationFramework(using val q: Quotes) {
  self =>
  import q.reflect.*
  import Validated.*

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

  case class Schema[A](
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

  object Schema:
    def forType[A](using Type[A]): Validated[String, Schema[A]] =
      val enc = 
        for
          enc <- sqlEncoding[A]
          trep: TypeRepr <- enc match
            case '{ $row: SQLRow[a] } => TypeRepr.of[a].asValid
            case _ =>
              s"Target type must derive ${TypeRepr.of[SQLRow].typeSymbol.name} to be qualified as a table.".err
        yield enc

      val sch = 
        for
          sch <- tableSchema[A]
          schema(tname, uniques*) = sch
          _ ~ fieldSpecs <- {
            val nameCheck =
              if !tname.isBlank then pass
              else "Schema name cannot be blank".err
            val fs = FieldSpec.allOf[A]
            nameCheck ~ fs
          }
          compUnique <- uniques.toList.distinct match
            case Nil => Valid(Nil)
            case us =>
              us.collect {
                case n if !fieldSpecs.exists(fs => fs.name == n) =>
                  s"Field `$n` defined in the composite unique constraint does not exist"
              }.asErrors(us)
          _ <- fieldSpecs match
            case Nil =>
              "The type designated as a table must be a `case class` with at least one field.".err
            case _ => pass
        yield Schema[A](tname, compUnique, fieldSpecs)

      enc ~ sch ^^ { case _ ~ s => s }

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
    ): Validated[String, FieldSpec[_ <: Any]] =
      val trep = TypeRepr.of[A]
      val caseFields = trep.typeSymbol.caseFields
      val companion = trep.typeSymbol.companionClass
      for
        si <- caseFields.zipWithIndex
          .find((cf, i) => cf.name == name)
          .toValid(s"Field $name does not exist in ${Type.show[A]}")
        (s, i) = si
        fieldType = trep.memberType(s)
        fs <- fieldType.asType match
          case '[t] =>
            val default = companion
              .declaredMethod(s"$$lessinit$$greater$$default$$${i + 1}")
              .headOption
              .map(Select(Ref(trep.typeSymbol.companionModule), _))
              .map(_.asExprOf[t])

            val enc = 
              for
                enc <- sqlEncoding[t]
                _ <- enc match
                  case '{ $e: SQLRow[a] } =>
                    s"Field ${s.name} cannot derive from ${TypeRepr.of[SQLRow].typeSymbol.name}.".err
                  case _ => pass
              yield enc

            val mtpe = 
              for
                m <- sqlMirror[t]
                mtpe = m.asTerm.tpe.asType match
                  case '[SQLMirror[t, x]] => Type.of[x]
              yield mtpe    
            
            val mods = List(
              annotationFor[name](s),
              annotationFor[pk](s),
              annotationFor[fk](s),
              annotationFor[autoinc](s),
              annotationFor[unique](s),
              annotationFor[index](s)
            ).sequence map (_.flatten)

            enc ~ mtpe ~ mods ^^ {
              case _ ~ mt ~ mo => FieldSpec[t](s.name, Type.of[t], mt, mo.toList, default)
            }
        _ <- {
          val nameCheck = if fs.sqlName.isBlank 
              then s"Provided SQL column name for the case field `${fs.name}` cannot be blank".err
              else pass
          val modsCheck = fs.mods
            .collect { case fk(tpe: Type[_], field, onDel) =>
              val v1 = (fs.tpe, onDel) match
                  case ('[Option[a]], OnDelete.SetNull) => pass
                  case (_, OnDelete.SetNull) =>
                      s"Field `${fs.name}` is not nullable; `OnDelete.SetNull` can only be specified for nullable columns".err
                  case _ => pass
              val v2 = tpe match
                  case '[a] =>
                    TypeRepr
                      .of[a]
                      .typeSymbol
                      .caseFields
                      .find(_.name == field) match
                      case Some(symb) =>
                        TypeRepr.of[a].memberType(symb) match
                          case reftrep if reftrep.no =:= fs.tpe.typeRepr.no => pass
                          case reftrep =>
                              (s"The Non-Option type of foreign key field `${fs.name}: ${Type
                                  .show(using fs.tpe.no)}` does not match that " +
                                s"of its referenced field `${field}: ${Type
                                    .show(using reftrep.no.asType)}` of the " +
                                s"case class `${Type.show(using tpe)}`").err
                      case None =>
                          s"Referenced foreign key field `$field` on record ${Type.show[a]} does not exist".err
              v1 ~ v2
            }.sequence
          nameCheck ~ modsCheck
        }
      yield fs

    def allOf[A](using
        Type[A]
    ): Validated[String, List[FieldSpec[_ <: Any]]] =
      val es =
        TypeRepr.of[A].typeSymbol.caseFields.map(cf => forField[A](cf.name))
      for
        specs <- es.sequence
        _ <- specs
          .groupBy(_.sqlName)
          .collect {
            case (n, acc) if acc.length > 1 => s"Duplicate column name: `$n`"
          }
          .toSeq
          .asErrors(())
      yield specs.toList

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

  def tableSchema[A](using ta: Type[A]): Validated[String, schema] =
    for
      ann <- annotationFor[schema](ta.typeRepr.typeSymbol)
      sch <- ann.toValid(
        s"Missing ${Type.show[schema]} annotation for type ${Type.show[A]}"
      )
    yield sch

  def tableName[A](using ta: Type[A]): Validated[String, String] =
    tableSchema[A].map(_.name)

  def annotationFor[T <: scala.annotation.StaticAnnotation](
      symb: Symbol
  )(using Type[T], FromExpr[T]): Validated[String, Option[T]] =
    for
      opt <- symb.getAnnotation(TypeRepr.of[T].typeSymbol).asValid
      v <- opt.fold(Valid(None)) { term =>
        Expr
          .unapply(term.asExprOf[T])
          .toValid(
            s"Static annotation ${Type.show[T]} for field ${symb.toString} cannot be unlifted. " +
              "Annotation must be constructed using literal values"
          )
          .map(Some(_))
      }
    yield v

  def sqlEncoding[A](using
      Type[A]
  ): Validated[String, Expr[SQLEncoding.Of[A]]] =
    Expr.summon[SQLEncoding.Of[A]] match
      case Some(enc) => enc.asValid
      case None =>
        s"Failed to obtain an instances of ${Type.show[SQLEncoding.Of[A]]}".err

  def sqlMirror[A](using Type[A]): Validated[String, Expr[SQLMirror.Of[A]]] =
    Expr.summon[SQLMirror.Of[A]] match
      case Some(enc) => enc.asValid
      case None =>
        "Failed to obtain an instances of ${Type.show[SQLMirror.Of[A]]}".err

  def validateSchema[A](using Type[A]): Validated[String, Schema[A]] =
    Schema.forType[A]

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

    def validSchema[T](using Type[T]): Schema[T] =
      self.validateSchema[T] match
        case Valid(n) => n
        case inv @ Invalid(_, _) =>
          throw GraceException(
            inv.errors
              .listString(s"Error validating schema for type ${Type.show[T]}")
              .get
          )

    def tableName[T](using Type[T]): String = tableSchema[T].name

    def tableSchema[T](using Type[T]): schema =
      self.tableSchema[T] match
        case Valid(n) => n
        case inv @ Invalid(_, _) =>
          throw GraceException(
            inv.errors
              .listString(s"Error obtaining schema for type ${Type.show[T]}")
              .get
          )

    def fieldSpecs[T](using Type[T]): List[FieldSpec[_]] =
      FieldSpec.allOf[T] match
        case Valid(specs) => specs
        case inv @ Invalid(_, _) =>
          throw GraceException(
            inv.errors
              .listString(
                s"Error obtaining field information for type ${Type.show[T]}"
              )
              .get
          )

    def fieldSpec[T](name: String)(using Type[T]): FieldSpec[_] =
      FieldSpec.forField[T](name) match
        case Valid(fs) => fs
        case inv @ Invalid(_, _) =>
          throw GraceException(
            inv.errors
              .listString(
                s"Error obtaining field information for type ${Type.show[T]}"
              )
              .get
          )
}
