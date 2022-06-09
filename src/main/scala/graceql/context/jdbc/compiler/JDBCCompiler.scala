package graceql.context.jdbc.compiler

import scala.quoted.*
import graceql.core.*
import graceql.context.jdbc.*
import graceql.quoted.CompileOps
import scala.util.Try

trait VendorTreeCompiler[V]:
  self =>

  def delegate[S[+X] <: Iterable[X]](using
      Quotes,
      Type[V],
      Type[S]
  ) : Delegate[S] = new Delegate[S]

  def tryCompile[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[[x] =>> Table[V, x], S, DBIO] ?=> A]
  )(using
      q: Quotes,
      ta: Type[A],
      ts: Type[S],
      tv: Type[V]
  ): Expr[Try[DBIO[A]]] =
    CompileOps.tryCompile(compile[S, A](e))

  def compile[S[+X] <: Iterable[X], A](
      e: Expr[Queryable[[x] =>> Table[V, x], S, DBIO] ?=> A]
  )(using
      q: Quotes,
      ta: Type[A],
      ts: Type[S],
      tv: Type[V]
  ): Expr[DBIO[A]] =
    import q.reflect.{Statement => _, *}
    e match
      case '{ (c: Queryable[[X] =>> Table[V, X], S, DBIO]) ?=> $body(c): A } =>
        delegate[S].compile[A](body)

  protected def binary(recurse: Node[Expr, Type] => Expr[String])(using
      Quotes
  ): PartialFunction[Node[Expr, Type], Expr[String]]

  def typeString[A](using q: Quotes)(tpe: Type[A]): Expr[String]

  class Delegate[S[+X] <: Iterable[X]](using
      override val q: Quotes,
      tv: Type[V],
      ts: Type[S]
  ) extends CompilationFramework(using q):
    import CompileOps.*
    import q.reflect.{
      Tree => _,
      Select => _,
      Block => _,
      Literal => SLiteral,
      Symbol => _,
      *
    }
    type Q = Queryable[[x] =>> Table[V, x], S, DBIO]
    given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

    private class NameGenerator(private val prefix: String):
      private var counter = 1
      def nextName(): String =
        synchronized {
          val id = counter
          counter += 1
          s"$prefix$id"
        }
    private val nameGen = NameGenerator("x")

    val partials: Seq[CompileModule[V, S]] = Seq(
      modules.NativeSyntaxSupport[V, S],
      modules.DDLSupport[V, S]
    )

    def compile[A](expr: Expr[Q => A])(using
        ta: Type[A]
    ): Expr[DBIO[A]] =

      val fallback: PartialFunction[Expr[Any], Node[Expr, Type]] = { case e =>
        throw GraceException(
          s"Unsupported operation!\n${e.asTerm.show(using Printer.TreeAnsiCode)}"
        )
      }
      def toNative(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]] =
        partials.foldRight(fallback) { (i, c) =>
          i(toNative, nameGen.nextName)(ctx).orElse(c)
        }

      val pipe =
        appliedToPlaceHolder[Q, A] andThen
          preprocess[A] andThen
          toNative(Context()) andThen
          typeCheck andThen
          toDBIO[A]
      pipe(expr)

    object checks:
      def schemas(tree: Node[Expr, Type]): Unit =
        tree
          .fold[List[TypeRepr]](Nil)(tps => {
            case Node.Table(_, tpe) => TypeRepr.of(using tpe) :: tps
          })
          .distinct
          .foldLeft[List[String]](Nil) { (msgs, trep) => 
              trep.asType match 
                case '[a] => validateSchema[a].swap.toOption.toList ++ msgs   
          } match
            case Nil  => ()
            case errs => throw GraceException(errs.mkString("\n"))

    protected def typeCheck(tree: Node[Expr, Type]): Node[Expr, Type] =
      checks.schemas(tree)
      tree.transform.pre {
        case Node.CreateTable(t @ Node.Table(_, tpe), None) =>
          tpe match
            case '[a] =>
              val specs = require.validSchema[a].forAST
              Node.CreateTable(t, Some(specs))
      }

    protected def toDBIO[A](
        tree: Node[Expr, Type]
    )(using ta: Type[A]): Expr[DBIO[A]] =
      import Node.*
      (ta, tree) match
        case ('[Unit], _: DropTable[_, _]) =>
          '{ DBIO.Statement(${ binary(tree) }) }.asExprOf[DBIO[A]]
        case ('[Unit], _: CreateTable[_, _]) =>
          '{ DBIO.Statement(${ binary(tree) }) }.asExprOf[DBIO[A]]
        case ('[Unit], _: Block[_, _]) =>
          '{ DBIO.Statement(${ binary(tree) }) }.asExprOf[DBIO[A]]
        case _ =>
          '{ DBIO.Query(${ binary(tree) }, (rs) => ???) }

    protected final def binary(tree: Node[Expr, Type]): Expr[String] =
      import Node.*
      self
        .binary(binary)
        .orElse {
          case DropTable(table) => '{ "DROP TABLE " + ${ binary(table) } }
          case CreateTable(table, Some(specs)) =>
            val specStrings = specs.map {
              case CreateSpec.ColDef(colName, tpe, mods) =>
                val modStrings = mods.map {
                  case ColMod.AutoInc() => '{ "AUTO_INCREMENT" }
                  case ColMod.Default(v) =>
                    '{ "DEFAULT " + ${ binary(Literal(v)) } }
                  case ColMod.NotNull() => '{ "NOT NULL" }
                  case ColMod.Unique()  => '{ "UNIQUE" }
                }
                '{
                  ${ Expr(colName) } + " " + ${ typeString(tpe) } + ${
                    Expr.ofList(modStrings)
                  }.mkString(" ", " ", "")
                }
              case CreateSpec.PK(columns) =>
                '{ "PRIMARY KEY (" + ${ Expr(columns.mkString(", ")) } + ")" }
              case CreateSpec.FK(
                    localCol,
                    remoteTableName,
                    remoteColName,
                    onDelete
                  ) =>
                val onDeleteStr = onDelete match
                  case OnDelete.Cascade    => '{ "CASCADE" }
                  case OnDelete.Restrict   => '{ "RESTRICT" }
                  case OnDelete.SetDefault => '{ "SET DEFAULT" }
                  case OnDelete.SetNull    => '{ "SET NULL" }
                '{
                  "FOREIGN KEY (" + ${
                    Expr(localCol)
                  } + ") REFERENCES " + $remoteTableName + "(" + ${
                    Expr(remoteColName)
                  } + ") ON DELETE " + $onDeleteStr
                }
              case CreateSpec.Index(indices) =>
                val indexStrings = indices.map {
                  case (c, Order.Asc)  => '{ ${ Expr(c) } + " ASC" }
                  case (c, Order.Desc) => '{ ${ Expr(c) } + " DESC" }
                }
                '{
                  "INDEX (" + ${ Expr.ofList(indexStrings) }
                    .mkString(", ") + ")"
                }
              case CreateSpec.Uniques(indices) =>
                val compUniques = indices.map(Expr(_))
                '{
                  "UNIQUE (" + ${ Expr.ofList(compUniques) }
                    .mkString(", ") + ")"
                }
            }

            '{
              "CREATE TABLE " + ${ binary(table) } + " (" + ${
                Expr.ofList(specStrings)
              }.mkString(", ") + ")"
            }
          case Select(
                distinct,
                columns,
                from,
                joins,
                where,
                groupBy,
                orderBy,
                offset,
                limit
              ) =>
            val distinctWords = if distinct then List('{ "DISTINCT" }) else Nil
            val columnsWords = List(binary(columns))
            val fromWords = from match
              case sub @ As(s: Select[_, _], name) =>
                List('{ "(" + ${ binary(s) } + ")" }, '{ "AS" }, Expr(name))
              case sub: Select[_, _] =>
                List('{ "(" + ${ binary(from) } + ")" })
              case i => List(binary(i))
            val joinWords = joins.map { case (jt, src, on) =>
              val jtStr = jt match
                case JoinType.Inner => "INNER"
                case JoinType.Left  => "LEFT"
                case JoinType.Right => "RIGHT"
                case JoinType.Full  => "FULL"
                case JoinType.Cross => "CROSS"
              List(
                Expr(jtStr),
                Expr("JOIN"),
                binary(src),
                Expr("ON"),
                binary(on)
              )
            }.flatten
            val whereWords = where.fold(Nil)(i => List('{ "WHERE" }, binary(i)))
            val groupByWords = groupBy.fold(List.empty) { case (cs, h) =>
              List(Expr("GROUP"), Expr("BY"), binary(cs)) ++ h
                .map(binary)
                .toList
            }
            val orderByWords = orderBy match
              case Nil => Nil
              case cs =>
                val ord = cs.map {
                  case (c, Order.Asc)  => '{ ${ binary(c) } + " ASC" }
                  case (c, Order.Desc) => '{ ${ binary(c) } + " DESC" }
                }
                List(
                  Expr("ORDER"),
                  Expr("BY"),
                  '{ ${ Expr.ofSeq(ord) }.mkString(", ") }
                )
            val limitWords =
              limit.fold(List.empty)(l => List(Expr("LIMIT"), binary(l)))
            val offsetWords =
              offset.fold(List.empty)(o => List(Expr("OFFSET"), binary(o)))
            val words =
              List('{ "SELECT" }) ++ distinctWords ++ columnsWords ++ List('{
                "FROM"
              }) ++ fromWords ++ joinWords ++ whereWords ++ groupByWords ++ orderByWords ++ limitWords ++ offsetWords

            '{ ${ Expr.ofList(words) }.mkString(" ") }
          case Block(stmts) =>
            '{
              ${ Expr.ofSeq(stmts.map(binary)) }.map(q => s"$q;").mkString(" ")
            }
          case Star() => '{ "*" }
          case Tuple(trees) =>
            '{ ${ Expr.ofSeq(trees.map(binary)) }.mkString(", ") }
          case As(tree, name) =>
            '{ ${ binary(tree) } + " AS " + ${ Expr(name) } }
          case Ref(name)       => Expr(name)
          case Column(name)    => Expr(name)
          case SelectCol(n, c) => '{ ${ binary(n) } + "." + ${ binary(c) } }
          case Table(name, _)  => name
          case Literal(value) =>
            value.asExprOf[scala.Any] match
              case '{ $o: Option[a] } =>
                '{ $o.fold("NULL") { i => ${ binary(Literal('{ i })) } } }
              case '{ $i: String } => '{ "\"" + $i + "\"" }
              case v               => '{ $v.toString }
          case Dual() => '{ "DUAL" }
          case FunApp(func, args, _) =>
            val encodedArgs = args.map { a =>
              a match
                case Literal(_) | SelectCol(_, _) |
                    FunApp(Func.Custom(_), _, _) | Star() =>
                  binary(a)
                case _ => '{ "(" + ${ binary(a) } + ")" }
            }
            (func, encodedArgs) match
              case (Func.BuiltIn(Symbol.Eq), List(l, r)) =>
                '{ $l + " = " + $r }
              case (Func.BuiltIn(Symbol.Neq), List(l, r)) =>
                '{ $l + " != " + $r }
              case (Func.BuiltIn(Symbol.Plus), List(l, r)) =>
                '{ $l + " + " + $r }
              case (Func.BuiltIn(Symbol.Minus), List(l, r)) =>
                '{ $l + " - " + $r }
              case (Func.BuiltIn(Symbol.Plus), List(l)) =>
                l
              case (Func.BuiltIn(Symbol.Minus), List(l)) =>
                '{ "-" + $l }
              case (Func.BuiltIn(Symbol.Mult), List(l, r)) =>
                '{ $l + " * " + $r }
              case (Func.BuiltIn(Symbol.And), List(l, r)) =>
                '{ $l + " AND " + $r }
              case (Func.BuiltIn(Symbol.Or), List(l, r)) =>
                '{ $l + " OR " + $r }
              case (Func.BuiltIn(Symbol.Count), List(arg)) =>
                '{ "COUNT(" + $arg + ")" }
              case (Func.Custom(name), as) =>
                '{
                  ${ Expr(name) } + "(" + ${ Expr.ofSeq(as) }
                    .mkString(", ") + ")"
                }
          case Cast(n, tpe) =>
            '{ "CAST(" + ${ binary(n) } + " AS " + ${ typeString(tpe) } + ")" }
        }(tree)

class Context(
    val refMap: Map[Any, String] = Map.empty[Any, String]
):
  import CompileOps.placeholder
  def withRegisteredIdent[A](using q: Quotes, ta: Type[A])(
      name: String
  ): (Context, Expr[A]) =
    import q.reflect.*
    val ident = newPlaceholder[A]
    (Context(refMap + (ident.asTerm -> name)), ident)

  def literalEncodable(using q: Quotes)(expr: Expr[Any]): Boolean =
    import q.reflect.*
    var encountered = false
    val symbol = Symbol.requiredMethod("graceql.quoted.CompileOps.placeholder")
    new q.reflect.TreeTraverser {
      override def traverseTree(tree: q.reflect.Tree)(owner: Symbol): Unit =
        tree match
          case i if i.symbol == symbol => encountered = true
          case _                       => super.traverseTree(tree)(owner)
    }.traverseTree(expr.asTerm)(Symbol.spliceOwner)
    !encountered

  def newPlaceholder[A](using Quotes, Type[A]): Expr[A] = '{ placeholder[A] }

  def isRegisteredIdent(using q: Quotes)(tree: q.reflect.Tree) =
    refMap.contains(tree)

  def refName(using q: Quotes)(tree: q.reflect.Tree): String =
    refMap(tree)

abstract class CompileModule[V, S[+X] <: Iterable[X]](using
    override val q: Quotes,
    val tv: Type[V],
    val ts: Type[S]
) extends CompilationFramework(using q):

  import q.reflect.*

  type Q = Queryable[[x] =>> Table[V, x], S, DBIO]
  given Type[Q] = Type.of[Queryable[[x] =>> Table[V, x], S, DBIO]]

  def apply(
      recurse: Context => Expr[Any] => Node[Expr, Type],
      nameGen: () => String
  )(ctx: Context): PartialFunction[Expr[Any], Node[Expr, Type]]
