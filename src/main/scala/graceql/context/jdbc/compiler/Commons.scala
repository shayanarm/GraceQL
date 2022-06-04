package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.context.jdbc.compiler.*
import graceql.quoted.CompileOps
import dotty.tools.dotc.reporting.trace.force

trait Commons(using val q: Quotes) {
  self =>
  import q.reflect.*

  case class FieldSpec[A](
      name: String,
      tpe: Type[A],
      nameOverride: Option[String],
      mods: Option[Modifiers],
      default: Option[Expr[A]]
  ):
    def resolvedName = nameOverride.getOrElse(name).trim
  object FieldSpec:
    def annotationFor[T <: scala.annotation.StaticAnnotation](symb: Symbol)(using Type[T], FromExpr[T]): Either[String, Option[T]] =
      throw GraceException(symb.annotations.map(_.show(using Printer.TreeAnsiCode)).toString)
      for
        opt <- Right(symb.getAnnotation(TypeRepr.of[T].typeSymbol))
        v <- opt.fold(Right(None)){term => 
          Expr.unapply(term.asExprOf[T])
          .toRight(s"Static annotation ${Type.show[T]} for field ${symb.toString} cannot be unlifted. Annotation must be constructed using literal values")
          .map(Some(_))  
        }
      yield v      
    def forType[A](using Type[A]): Either[List[String], List[FieldSpec[_ <: Any]]] =
      val trep = TypeRepr.of[A]
      val caseFields = trep.typeSymbol.caseFields
      val companion = trep.typeSymbol.companionClass
      val fields: List[Either[List[String], FieldSpec[_ <: Any]]] = caseFields.zipWithIndex.map((s, i) =>
        val fieldType = trep.memberType(s)
        fieldType.asType match
          case '[t] =>
            val default = companion
              .declaredMethod(s"$$lessinit$$greater$$default$$${i + 1}")
              .headOption
              .map(Select(Ref(trep.typeSymbol.companionModule), _))
              .map(_.asExprOf[t])
            
            val nameOverride = annotationFor[Name](s).map(_.map(_.name))
            val mods = annotationFor[Modifiers](s)
            
            (nameOverride, mods) match
              case (Right(no), Right(ms)) => Right(FieldSpec[t](s.name, Type.of[t], no, ms, default))
              case (Left(e1), Left(e2)) => Left(List(e1, e2))
              case (Left(e), _) => Left(List(e))
              case (_, Left(e)) => Left(List(e))
      )

      fields.foldRight[Either[List[String], List[FieldSpec[_ <: Any]]]](Right(Nil)) {(i, c) => 
        (c, i) match
          case (Right(fs), Right(f)) => Right(f :: fs)  
          case (Left(es), Left(e)) => Left(e ++ es)
          case (l@Left(_), Right(_)) => l
          case (Right(_), Left(e)) => Left(e)
      }
    def forAST(specs: List[FieldSpec[_]]): List[CreateSpec[Expr, Type]] = 
      val colDefs = specs.map {
         case fs@ FieldSpec(_, tpe, _ , mods, default) => 
          val astDefault = default.map(ColMod.Default(_))           
          tpe match
            case '[Option[a]] => 
              CreateSpec.ColDef[Expr, Type, a](
                  fs.resolvedName,
                  Type.of[a],
                  for
                    ms <- mods.toList
                    d <- ms.values.collect {
                      case Modifier.AutoIncrement => ColMod.AutoInc[Expr]()
                    } ++ astDefault.toList
                  yield d  
                )
            case '[a] =>
              CreateSpec.ColDef[Expr, Type, a](
                  fs.resolvedName,
                  Type.of[a],
                  for
                    ms <- mods.toList
                    d <- ms.values.collect {
                      case Modifier.AutoIncrement => ColMod.AutoInc[Expr]()
                    } ++ List(ColMod.NotNull[Expr]()) ++ astDefault.toList
                  yield d  
                )                             
      }
      val pk: Option[CreateSpec[Expr, Type]] = 
        (for
          spec <- specs
          mod <- spec.mods.toList
          v <- mod.values.collect {
              case Modifier.PrimaryKey => spec.resolvedName
            }.take(1)
        yield v) match
          case Nil => None
          case ks => Some(CreateSpec.PK(ks))
      
      val indices: Option[CreateSpec[Expr, Type]] = 
        (for
          spec <- specs
          mod <- spec.mods.toList
          v <- mod.values.collect {
              case Modifier.Indexed(ord) => (spec.resolvedName, ord)
            }.take(1)
        yield v) match
          case Nil => None
          case idxs => Some(CreateSpec.Index(idxs))

      val uniques: Option[CreateSpec[Expr, Type]] =       
        (for
          spec <- specs
          mod <- spec.mods.toList
          v <- mod.values.collect {
              case Modifier.Unique => spec.resolvedName
            }.take(1)
        yield v) match
          case Nil => None
          case ks => Some(CreateSpec.Unique(ks))
      val fks: List[CreateSpec[Expr, Type]] = (for
          spec <- specs
          mod <- spec.mods.toList
          v <- mod.values.collect {
              case Modifier.ForeignKey(tpe: Type[_], ref, onDel) => 
                tpe match
                  case '[a] => CreateSpec.FK[Expr, Type](spec.resolvedName, Expr(require.tableName[a]), ref, onDel)
            }.take(1)
        yield v)    
      colDefs ++ pk.toList ++ fks ++ indices.toList ++ uniques.toList    

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

  def tableName[A](using ta: Type[A]): Either[String, String] =
    for
      annTerm <- TypeRepr
        .of(using ta)
        .typeSymbol
        .getAnnotation(TypeRepr.of[Name].typeSymbol)
        .toRight(
          s"Missing ${Type.show[Name]} annotation for type ${Type.show[A]}"
        )
      ann <- Expr
        .unapply(annTerm.asExprOf[Name])
        .toRight(
          s"Static annotation ${Type.show[Name]} for type ${Type.show[A]} cannot be unlifted. Annotation must be constructed using literal values"
        )
    yield ann.name

  def schemaErrors[A](using Type[A]): Option[String] =
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
      tname <- tableName[A].left.map(List(_))
      fieldSpecs <- FieldSpec.forType[A]
      _ <- fieldSpecs match
        case Nil =>
          Left(
            List("Target type must be a `case class` with atleast one field.")
          )
        case fields => Right(fields)
      _ <-
        val columnTypeErr = fieldSpecs.exists {
          case FieldSpec(_, tr, _, _, d) =>
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
          val err =
            if fs.resolvedName.isBlank then
              Some(
                s"Resolved SQL column name for the case field ${fs.name} cannot be blank"
              )
            else None
          err.toList
        }
        val modErrs: List[String] = 
          for
            fs <- fieldSpecs
            mods <- fs.mods.toList
            mod <- mods.values
            err <- Nil
          yield err

        columnTypeErr.toList ++ fieldNameErrors ++ modErrs match
          case Nil  => Right(())
          case errs => Left(errs)
    yield ()
    r.swap.toOption.map(
      _.map(e => s"-  $e").mkString(
        s"Schema validation failed for type ${Type.show[A]}:\n",
        "\n",
        ""
      )
    )

  object assertions:
    def validSchema[A](using Type[A]): Unit =
      self.schemaErrors[A].foreach(report.errorAndAbort)

  object require:
    def instance[T](using Type[T]): Expr[T] =
      Expr.summon[T] match
        case Some(i) => i
        case None =>
          report.errorAndAbort(
            s"Could not obtain an instance for ${Type.show[T]}"
          )
    def tableName[T](using Type[T]): String =
      self.tableName[T] match
        case Right(n)  => n
        case Left(err) => report.errorAndAbort(err)
    def fieldSpecs[T](using Type[T]): List[FieldSpec[_]] = 
      FieldSpec.forType[T] match
        case Right(specs) => specs
        case Left(errs) => report.errorAndAbort(
          errs.map(e => s"-  $e").mkString(s"Error fetching field information for type ${Type.show[T]}:\n", "\n", "")
        )    

  object errorStrings:
    def sqlEncodingNotFound[A](using Type[A]): String =
      s"No instance of ${Type.show[SQLEncoding.Of[A]]} found for type ${Type.show[A]}"

}
