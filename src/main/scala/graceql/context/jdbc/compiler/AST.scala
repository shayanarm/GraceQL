package graceql.context.jdbc

import graceql.core.*
import scala.util.parsing.combinator._
import scala.util.Try
import graceql.core.GraceException
import scala.util.matching.Regex

enum JoinType:
  case Inner extends JoinType
  case Left extends JoinType
  case Right extends JoinType
  case Full extends JoinType
  case Cross extends JoinType

enum Order:
  case Asc extends Order
  case Desc extends Order

enum Func:
  case BuiltIn(symbol: Symbol) extends Func
  case Custom(name: String) extends Func

enum Symbol:
  // unary
  case Not extends Symbol // NOT
  case All extends Symbol // ALL
  case Any extends Symbol // Any
  case IsNull extends Symbol // IS NULL
  case IsNotNull extends Symbol // IS NOT NULL
  case Exists extends Symbol // EXISTS
  // binary
  case Minus extends Symbol // +
  case Plus extends Symbol // -
  case Mult extends Symbol // *
  case Div extends Symbol ///
  case Mod extends Symbol // %
  case Eq extends Symbol // =
  case Neq extends Symbol // !=
  case Gt extends Symbol // >
  case Gte extends Symbol // >=
  case Lt extends Symbol // <
  case Lte extends Symbol // <=
  case BWAnd extends Symbol // &
  case BWOr extends Symbol // |
  case BWXor extends Symbol // ^
  case And extends Symbol // AND
  case Or extends Symbol // OR
  case In extends Symbol // IN
  case Like extends Symbol // LIKE
  // ternary
  case Between extends Symbol // BETWEEN
  // aggregate
  case Count extends Symbol //COUNT
  case Sum extends Symbol //SUM
  case Min extends Symbol //MIN
  case Max extends Symbol //MAX
  case Avg extends Symbol //AVG
  case First extends Symbol //FIRST


type Join[L[_], T[_]] = (JoinType, Node[L, T], Node[L, T])
type Ordered[L[_], T[_]] = (Node[L, T], Order)
type GroupBy[L[_], T[_]] = (Node[L, T], Option[Node[L, T]])

enum CreateSpec[L[_],T[_]]:
  case ColDef[L[_], T[_], A](name: String, tpe: T[A], modifiers: List[ColMod[L]]) extends CreateSpec[L,T]
  case PK[L[_], T[_]](columns: List[String]) extends CreateSpec[L,T]
  case FK[L[_], T[_]](localCol: String, remoteTableName: L[String], remoteColName: String, onDelete: OnDelete) extends CreateSpec[L,T]
  case Index[L[_], T[_]](indices: List[(String, Order)]) extends CreateSpec[L,T]

enum ColMod[L[_]]:
  case NotNull[L[_]]() extends ColMod[L]
  case AutoInc[L[_]]() extends ColMod[L]
  case Default[L[_], A](value: L[A]) extends ColMod[L]

enum OnDelete:
  case Cascade extends OnDelete
  case SetDefault extends OnDelete
  case SetNull extends OnDelete
  case Restrict extends OnDelete


enum Node[L[_], T[_]]:
  node =>
  case Literal[L[_], T[_], A](value: L[A]) extends Node[L, T]
  case Select[L[_], T[_]](
      distinct: Boolean,
      columns: Node[L, T],
      from: Node[L, T],
      joins: List[Join[L, T]],
      where: Option[Node[L, T]],
      groupBy: Option[GroupBy[L, T]],
      orderBy: List[Ordered[L, T]],
      offset: Option[Node[L, T]],
      limit: Option[Node[L, T]]
  ) extends Node[L, T]
  case Star() extends Node[L, T]
  case Column(name: String) extends Node[L, T]
  case Tuple[L[_], T[_]](columns: List[Node[L, T]]) extends Node[L, T]
  case Table[L[_], T[_], A](table: L[String], tpe: T[A]) extends Node[L, T]
  case Values[L[_], T[_], A](values: L[Iterable[A]]) extends Node[L, T]
  case Dual() extends Node[L, T]
  case As[L[_], T[_]](tree: Node[L, T], alias: String) extends Node[L, T]
  case Ref(name: String) extends Node[L, T]
  case SelectCol[L[_], T[_]](tree: Node[L, T], selection: Node[L, T])
      extends Node[L, T]
  case FunApp[L[_], T[_], A](
      name: Func,
      args: List[Node[L, T]],
      returnType: Option[T[A]]
  ) extends Node[L, T]
  case TypeLit[L[_], T[_], A](tpe: T[A]) extends Node[L, T]
  case Cast[L[_], T[_], A](tree: Node[L, T], tpe: T[A]) extends Node[L, T]
  case TypeAnn[L[_], T[_], A](tree: Node[L, T], tpe: T[A]) extends Node[L, T]
  case Null() extends Node[L, T]
  case Any[L[_], T[_]](
      column: Node[L, T],
      operator: Symbol,
      subquery: Node[L, T]
  ) extends Node[L, T]
  case All[L[_], T[_]](
      column: Node[L, T],
      operator: Symbol,
      subquery: Node[L, T]
  ) extends Node[L, T]
  case Union[L[_], T[_]](left: Node[L, T], right: Node[L, T]) extends Node[L, T]
  case Block[L[_], T[_]](stmts: List[Node[L, T]]) extends Node[L, T]
  case DropTable[L[_], T[_]](tableName: L[String]) extends Node[L, T]
  case CreateTable[L[_], T[_]](tableName: L[String], specs: List[CreateSpec[L,T]]) extends Node[L, T]

  object transform:
    def pre(f: PartialFunction[Node[L, T], Node[L, T]]): Node[L, T] =
      val g = f.orElse { case t => t }
      g(node) match
        case Literal(_) => node
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
          Select(
            distinct,
            columns.transform.pre(f),
            from.transform.pre(f),
            joins.map { case (jt, s, o) =>
              (jt, s.transform.pre(f), o.transform.pre(f))
            },
            where.map(_.transform.pre(f)),
            groupBy.map { case (es, h) =>
              (es.transform.pre(f), h.map(_.transform.pre(f)))
            },
            orderBy.map { case (t, o) => (t.transform.pre(f), o) },
            offset.map(_.transform.pre(f)),
            limit.map(_.transform.pre(f))
          )
        case Star()          => node
        case Column(_)       => node
        case Tuple(elems)    => Tuple(elems.map(_.transform.pre(f)))
        case Table(_, _)     => node
        case Values(_)       => node
        case Dual()          => node
        case As(t, n)        => As(t.transform.pre(f), n)
        case Ref(_)          => node
        case SelectCol(t, c) => SelectCol(t.transform.pre(f), c.transform.pre(f))
        case FunApp(n, args, tpe) =>
          FunApp(n, args.map(_.transform.pre(f)), tpe)
        case TypeLit(_)      => node
        case Cast(t, tpe)    => Cast(t.transform.pre(f), tpe)
        case TypeAnn(t, tpe) => TypeAnn(t.transform.pre(f), tpe)
        case Null()          => node
        case Any(c, o, s) =>
          Any(c.transform.pre(f), o, s.transform.pre(f))
        case All(c, o, s) =>
          All(c.transform.pre(f), o, s.transform.pre(f))
        case Union(l, r) =>
          Union(l.transform.pre(f), r.transform.pre(f))
        case Block(stmts) => Block(stmts.map(_.transform.pre(f)))
        case DropTable(_) => node
        case CreateTable(_, _) => node

    def post(f: PartialFunction[Node[L, T], Node[L, T]]): Node[L, T] =
      val g = f.orElse { case t => t }
      val n = node match
        case Literal(_) => node
        case Select(
              distinct,
              columns,
              from,
              joins,
              where,
              groupBy,
              orderBy,
              limit,
              offset
            ) =>
          Select(
            distinct,
            columns.transform.post(f),
            from.transform.post(f),
            joins.map { case (jt, s, o) =>
              (jt, s.transform.post(f), o.transform.post(f))
            },
            where.map(_.transform.post(f)),
            groupBy.map { case (es, h) =>
              (es.transform.post(f), h.map(_.transform.post(f)))
            },
            orderBy.map { case (t, o) => (t.transform.post(f), o) },
            offset.map(_.transform.post(f)),
            limit.map(_.transform.post(f))
          )
        case Star()          => node
        case Column(_)       => node
        case Tuple(elems)    => Tuple(elems.map(_.transform.post(f)))
        case Table(_, _)     => node
        case Values(_)       => node
        case Dual()          => node
        case As(t, n)        => As(t.transform.post(f), n)
        case Ref(_)          => node
        case SelectCol(t, c) => SelectCol(t.transform.post(f), c.transform.pre(f))
        case FunApp(n, args, tpe) =>
          FunApp(n, args.map(_.transform.post(f)), tpe)
        case TypeLit(_)      => node
        case Cast(t, tpe)    => Cast(t.transform.post(f), tpe)
        case TypeAnn(t, tpe) => TypeAnn(t.transform.post(f), tpe)
        case Null()          => node
        case Any(c, o, s) =>
          Any(c.transform.post(f), o, s.transform.post(f))
        case All(c, o, s) =>
          All(c.transform.post(f), o, s.transform.post(f))
        case Union(l, r) =>
          Union(l.transform.post(f), r.transform.post(f))
        case Block(stmts) => Block(stmts.map(_.transform.post(f)))
        case DropTable(_) => node
        case CreateTable(_, _) => node
      g(n)

    inline def lits[L2[_]](f: [A] => L[A] => L2[A]): Node[L2, T] =
      both(f, [x] => (i: T[x]) => i)
    inline def types[T2[_]](f: [A] => T[A] => T2[A]): Node[L, T2] =
      both([x] => (i: L[x]) => i, f)
    def both[L2[_], T2[_]](
        fl: [X] => L[X] => L2[X],
        ft: [Y] => T[Y] => T2[Y]
    ): Node[L2, T2] =
      node match
        case Literal(l) => Literal(fl(l))
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
          Select(
            distinct,
            columns.transform.both(fl, ft),
            from.transform.both(fl, ft),
            joins.map { case (jt, s, on) =>
              (jt, s.transform.both(fl, ft), on.transform.both(fl, ft))
            },
            where.map(_.transform.both(fl, ft)),
            groupBy.map { case (e, h) =>
              (e.transform.both(fl, ft), h.map(_.transform.both(fl, ft)))
            },
            orderBy.map { case (t, o) => (t.transform.both(fl, ft), o) },
            offset.map(_.transform.both(fl, ft)),
            limit.map(_.transform.both(fl, ft))
          )
        case Star()          => Star()
        case Column(n)       => Column(n)
        case Tuple(elems)    => Tuple(elems.map(_.transform.both(fl, ft)))
        case Table(n, t)     => Table(fl(n), ft(t))
        case Values(ls)      => Values(fl(ls))
        case Dual()          => Dual()
        case As(t, n)        => As(t.transform.both(fl, ft), n)
        case Ref(n)          => Ref(n)
        case SelectCol(t, c) => SelectCol(t.transform.both(fl, ft), c.transform.both(fl, ft))
        case FunApp(n, args, tpe) =>
          FunApp(
            n,
            args.map(_.transform.both(fl, ft)),
            tpe.map(i => ft(i))
          )
        case TypeLit(tpe)    => TypeLit(ft(tpe))
        case Cast(t, tpe)    => Cast(t.transform.both(fl, ft), ft(tpe))
        case TypeAnn(t, tpe) => TypeAnn(t.transform.both(fl, ft), ft(tpe))
        case Null()          => Null()
        case Any(c, o, s) =>
          Any(
            c.transform.both(fl, ft),
            o,
            s.transform.both(fl, ft)
          )
        case All(c, o, s) =>
          All(
            c.transform.both(fl, ft),
            o,
            s.transform.both(fl, ft)
          )
        case Union(l, r) =>
          Union(
            l.transform.both(fl, ft),
            r.transform.both(fl, ft)
          )
        case Block(stmts) => Block(stmts.map(_.transform.both(fl, ft)))
        case DropTable(n) => DropTable(fl(n))
        case CreateTable(n,specs) => CreateTable(fl(n), specs.map {
          case CreateSpec.ColDef(name, tpe, mods) =>
            CreateSpec.ColDef(name, ft(tpe), mods.map{
              case ColMod.NotNull() => ColMod.NotNull()
              case ColMod.AutoInc() => ColMod.AutoInc()
              case ColMod.Default(v) => ColMod.Default(fl(v))
            })            
          case CreateSpec.PK(ks) => CreateSpec.PK(ks)
          case CreateSpec.FK(l, tname, r, od) => CreateSpec.FK(l, fl(tname), r, od)
          case CreateSpec.Index(is) => CreateSpec.Index(is)
        })

object Node:
  inline def parse[L[_], T[_]](sc: StringContext)(
      args: Seq[Node[L, T]]
  ): Try[Node[L, T]] =
    val placeholders = args.indices.map(i => s":$i")
    val raw = sc.raw(placeholders*)
    parser(args.toArray).apply(raw)
  extension (sc: StringContext)
    inline def gensql[L[_], T[_]](args: Node[L, T]*): Node[L, T] =
      parse(sc)(args).get

  inline def parser[L[_], T[_]](args: Array[Node[L, T]]): SQLParser[L, T] =
    SQLParser[L, T](args)

class SQLParser[L[_], T[_]](val args: Array[Node[L, T]]) extends RegexParsers:
  private type N = Node[L, T]
  import Node.*
  import SQLParser.*
            
  object operand:
    def - : Parser[Func] = "-" ^^ { _ => Func.BuiltIn(Symbol.Minus) }
    def + : Parser[Func] = "+" ^^ { _ => Func.BuiltIn(Symbol.Plus) }
    def * : Parser[Func] = "*" ^^ { _ => Func.BuiltIn(Symbol.Mult) }
    def / : Parser[Func] = "/" ^^ { _ => Func.BuiltIn(Symbol.Div) }
    def % : Parser[Func] = "%" ^^ { _ => Func.BuiltIn(Symbol.Mod) }
    def `=` : Parser[Func] = "=" ^^ { _ => Func.BuiltIn(Symbol.Eq) }
    def != : Parser[Func] = ("!=" | "<>") ^^ { _ => Func.BuiltIn(Symbol.Neq) }
    def > : Parser[Func] = ">" ^^ { _ => Func.BuiltIn(Symbol.Gt) }
    def >= : Parser[Func] = ">=" ^^ { _ => Func.BuiltIn(Symbol.Gte) }
    def < : Parser[Func] = "<" ^^ { _ => Func.BuiltIn(Symbol.Lt) }
    def <= : Parser[Func] = "<=" ^^ { _ => Func.BuiltIn(Symbol.Lte) }
    def & : Parser[Func] = "&" ^^ { _ => Func.BuiltIn(Symbol.BWAnd) }
    def | : Parser[Func] = "|" ^^ { _ => Func.BuiltIn(Symbol.BWOr) }
    def && : Parser[Func] = "&&" ^^ { _ => Func.BuiltIn(Symbol.And) }
    def || : Parser[Func] = "||" ^^ { _ => Func.BuiltIn(Symbol.Or) }
    def ^ : Parser[Func] = "^" ^^ { _ => Func.BuiltIn(Symbol.BWOr) }
    def logicals: List[Parser[Func]] = List(`=`, !=, >, >=, <, <=)

  object embedded:
    def apply[A](
        f: PartialFunction[N, A],
        nodeName: String
    ) = (""":""".r ~> """\d+""".r) ^^ { i => args(i.toInt) } ^? (f, t =>
      s"Expected \"${nodeName}\" reference, but found $t")

    def table: Parser[N] = apply(
      { case t: Table[_, _, _] => t },
      "Table"
    )

    def block: Parser[N] = apply(
      { case t: Block[_, _] => t },
      "Block"
    )

    def literal: Parser[N] = apply(
      { case t: Literal[_, _, _] => t },
      "Literal"
    )

    def selectQuery: Parser[N] = apply(
      { case t: Select[_, _] => t },
      "Select"
    )

    def tuple: Parser[N] = apply(
      { case t: Tuple[_, _] => t },
      "Tuple"
    )

    def funApp: Parser[N] = apply(
      { case t: FunApp[_, _, _] => t },
      "FunApp"
    )

    def ref: Parser[N] = apply(
      { case t: Ref[_, _] => t },
      "Ref"
    )

    def values: Parser[N] = apply(
      { case t: Values[_, _, _] => t },
      "Values"
    )

    def dual: Parser[N] = apply(
      { case t: Dual[_, _] => t },
      "Dual"
    )

    def star: Parser[N] = apply(
      { case t: Star[_, _] => t },
      "Star"
    )

    def selectCol: Parser[N] = apply(
      { case t: SelectCol[_, _] => t },
      "SelectCol"
    )   

    def column: Parser[N] = apply(
      { case t: Column[_, _] => t },
      "Column"
    )   

    def typeLit: Parser[N] = apply(
      { case t: TypeLit[_, _, _] => t },
      "TypeLit"
    )   

  object selectQuery extends Parser[N]:
    def join: Parser[Join[L, T]] =
      def inner = kw.inner ~> kw.join ^^ { case _ => JoinType.Inner }
      def left = kw.left ~> kw.join ^^ { case _ => JoinType.Left }
      def right = kw.right ~> kw.join ^^ { case _ => JoinType.Right }
      def full = kw.full ~> kw.join ^^ { case _ => JoinType.Full }
      def cross = kw.cross ~> kw.join ^^ { case _ => JoinType.Cross }
      (inner | left | right | full | cross) ~ src ~ (kw.on ~> expr) ^^ {
        case jt ~ s ~ e => (jt, s, e)
      }
    def where: Parser[N] = kw.where ~> expr
    def groupBy: Parser[GroupBy[L,T]] = kw.group ~> kw.by ~> tuple(expr) ~ (kw.having ~> expr).? ^^ {case es ~ h => (es, h)}
    def orderBy: Parser[List[Ordered[L,T]]] = 
      kw.order ~> kw.by ~> rep1sep(expr ~ ord, kw.`,`) ^^ {es =>
        es.map{case e ~ o => (e, o)}
      }
    def offset: Parser[N] = kw.offset ~> expr
    def limit: Parser[N] = kw.limit ~> expr
    def selectColumns: Parser[N] =
      tuple(aliased(false)(expr)) | star

    def apply(v: Input): ParseResult[N] =
      def written =
        def comutativeOL = 
          def lim = limit >> {l => success(Some(l)) ~ success(Option.empty[N])}
          def off = offset >> {o => success(Option.empty[N]) ~ success(Some(o))}
          def lo = limit ~ offset >> {case l ~ o => success(Some(l)) ~ success(Some(o)) }
          def ol = offset ~ limit >> {case o ~ l => success(Some(l)) ~ success(Some(o)) }
          def none = success(None) ~ success(None)
          lo | ol | lim | off | none
        kw.select ~> kw.distinct.? ~ selectColumns ~ (kw.from ~> src) ~ rep(join) ~ where.? ~ groupBy.? ~ orderBy.? ~ comutativeOL ^^ {
          case distinct ~ colset ~ from ~ joins ~ where ~ groupBy ~ orderBy ~ (limit ~ offset) =>
            Select(
              distinct.isDefined,
              colset,
              from,
              joins,
              where,
              groupBy,
              orderBy.to(List).flatten,
              offset,
              limit
            )
        }
      (embedded.selectQuery | written)(v)
  def src: Parser[N] =
    aliased(false)(
      embedded.table | subquery | embedded.values
    ) | dual    
  def parens[A](required: Boolean)(without: => Parser[A]): Parser[A] =
    val `with` = "(" ~> parens(false)(without) <~ ")"
    if required then `with` else `with` | without
  def dual: Parser[N] = embedded.dual | kw.dual ^^ { _ => Dual() }
  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^? ({
    case p if !kw.registry.values.exists(r => r.matches(p)) => p
  }, t => s"Keyword \"${t}\" cannot be used as identifier")
  def ref: Parser[N] =
    embedded.ref | ident ^^ { r => Ref(r) }
  def ord: Parser[Order] = 
    def asc = kw.asc ^^ {_ => Order.Asc}
    def desc = kw.desc ^^ {_ => Order.Desc}    
    (asc | desc).? ^^ (_.getOrElse(Order.Asc))    
  def updateQuery: Parser[N] = ???
  def deleteQuery: Parser[N] = ???
  def insertQuery: Parser[N] = ???

  object createQuery extends Parser[N]:
    def columnDef: Parser[CreateSpec[L,T]] = 
      expr.column ~ embedded.typeLit ~ rep(columnModifier) ^^ {case Column(c) ~ TypeLit(tpe) ~ ms => CreateSpec.ColDef(c, tpe, ms)}
    def fk: Parser[CreateSpec[L,T]] = 
      def onDel: Parser[OnDelete] = 
        def cascade = kw.cascade ^^ {_ => OnDelete.Cascade}
        def setNull = kw.set ~ kw.`null` ^^ {_ => OnDelete.SetNull}
        def setDefault = kw.set ~ kw.default ^^ {_ => OnDelete.SetDefault}
        def restrict = kw.restrict ^^ {_ => OnDelete.Restrict}
        cascade | setNull | setDefault | restrict
      kw.foreign ~> kw.key ~> parens(true)(expr.column) ~ (kw.references ~> embedded.table ~ parens(true)(expr.column)) ~ (kw.on ~> kw.delete ~> onDel).? ^^ { case Column(l) ~ (Table(n, _) ~ Column(r)) ~ od =>
        CreateSpec.FK(l, n, r, od.getOrElse(OnDelete.Restrict))
      }
    def pk: Parser[CreateSpec[L,T]] = 
      kw.primary ~> kw.key ~> parens(true)(rep1sep(expr.column, kw.`,`)) ^^ {cs => CreateSpec.PK(cs.map{case Column(str) => str})}
    def index: Parser[CreateSpec[L,T]] = kw.index ~> parens(true)(rep1sep(expr.column ~ ord, kw.`,`)) ^^ {is => 
      CreateSpec.Index(is.map{case Column(c) ~ o => (c, o)})
    }

    def columnModifier: Parser[ColMod[L]] = 
      def autoInc: Parser[ColMod[L]] = kw.auto_increment ^^ {_ => ColMod.AutoInc()}
      def notNull: Parser[ColMod[L]] = kw.not ~> kw.`null` ^^ {_ => ColMod.NotNull()}
      def default: Parser[ColMod[L]] = kw.default ~> embedded.literal ^^ {case Literal(l) => ColMod.Default(l)}
      autoInc | notNull | default

    def columnSpec: Parser[CreateSpec[L,T]] = fk | pk | index | columnDef
    def apply(v: Input): ParseResult[N] =
      def parser = kw.create ~> kw.table ~> embedded.table ~ parens(true)(rep1sep(columnSpec, kw.`,`)) ^^ {case Table(n,_) ~ specs => CreateTable(n,specs)}
      parser(v)

  def dropQuery: Parser[N] = kw.drop ~> kw.table ~> embedded.table ^^ {case Table(n,_) => DropTable(n)}
  def block: Parser[N] =
    def written = rep1sep(query, kw.`;`) ^^ {
      case Nil      => Block(Nil)
      case h +: Nil => h
      case l        => Block(l)
    }
    embedded.block | written
  def tuple(of: => Parser[N]): Parser[N] =
    embedded.tuple | rep1sep(of, kw.`,`) ^^ { r => Tuple(r) }

  def star: Parser[N] = embedded.star | kw.star ^^ { _ => Star() }

  def aliased(required: Boolean)(
      of: => Parser[N]
  ): Parser[N] =
    val alias = kw.as.? ~> ident
    required match
      case false =>
        of ~ alias.? ^^ {
          case o ~ Some(i) => As(o, i)
          case o ~ _       => o
        }
      case true => of ~ alias ^^ { case o ~ i => As(o, i) }

  def sql: Parser[N] = block | query | expr
  def query: Parser[N] =
    createQuery| dropQuery | selectQuery // | updateQuery | deleteQuery | insertQuery
  def subquery: Parser[N] = parens(false)(
    embedded.selectQuery
  ) | parens(true)(selectQuery)

  object expr extends Parser[N]:
    def not: Parser[N => N] = kw.not ^^ { case _ =>
      n => FunApp(Func.BuiltIn(Symbol.Not), List(n), None)
    }
    def all: Parser[N => N] =
      operand.logicals.reduce(_ | _) ~ (kw.all ~> subquery) ^^ {
        case Func.BuiltIn(symbol) ~ q => l => All(l, symbol, q)
      }
    def any: Parser[N => N] =
      operand.logicals.reduce(_ | _) ~ ((kw.any | kw.some) ~> subquery) ^^ {
        case Func.BuiltIn(symbol) ~ q => l => Any(l, symbol, q)
      }
    def nullCheck: Parser[N => N] = (kw.is ~> kw.not.?) <~ kw.`null` ^^ {
      b => n =>
        b match
          case Some(_) => FunApp(Func.BuiltIn(Symbol.IsNotNull), List(n), None)
          case None    => FunApp(Func.BuiltIn(Symbol.IsNull), List(n), None)
    }
    def exists: Parser[N] = kw.exists ~> subquery ^^ { case n =>
      FunApp(Func.BuiltIn(Symbol.Exists), List(n), None)
    }
    def column: Parser[N] = 
      def written = """[a-zA-Z][a-zA-Z0-9_]*""".r ^^ { name => 
        Column[L,T](name)  
      }
      written | embedded.column
    def selectCol: Parser[N] = 
      def written = ident ~ (kw.`.` ~> (column | star)) ^^ {case id ~ c => 
          SelectCol[L,T](Ref(id),c)  
        }
      written | embedded.selectCol
    def customFunction: Parser[N] =
      ident ~ parens(true)(repsep(expr, kw.`,`)) ^^ { case name ~ args =>
        FunApp(Func.Custom(name), args, None)
      } 
    def aggregateFunction: Parser[N] =
      def count = kw.count map (_ => Symbol.Count)
      def sum = kw.sum map (_ => Symbol.Sum)
      def min = kw.min map (_ => Symbol.Min)
      def max = kw.max map (_ => Symbol.Max)
      def avg = kw.avg map (_ => Symbol.Avg)
      def first = kw.first map (_ => Symbol.First)
      (count | sum | min | max | avg | first) ~ parens(true)(expr | star) ^^ { case symb ~ arg =>
        FunApp(Func.BuiltIn(symb), List(arg), None)
      }
    def cast: Parser[N] = kw.cast ~> parens(true)(expr ~ (kw.as ~> embedded.typeLit)) ^^ {case n ~ TypeLit(tpe) => Cast(n,tpe)}
    def and: Parser[N => N] = (kw.and ~> prec1) ^^ { case r =>
      l => FunApp(Func.BuiltIn(Symbol.And), List(l, r), None)
    }
    def or: Parser[N => N] = (kw.or ~> prec1) ^^ { case r =>
      l => FunApp(Func.BuiltIn(Symbol.Or), List(l, r), None)
    }
    def in: Parser[N => N] = (kw.in ~> (embedded.values | subquery)) ^^ {
      case r => l => FunApp(Func.BuiltIn(Symbol.In), List(l, r), None)
    }
    def like: Parser[N => N] = kw.like ~> prec1 ^^ { case r =>
      l => FunApp(Func.BuiltIn(Symbol.Like), List(l, r), None)
    }
    def between: Parser[N => N] = (kw.between ~> prec1) ~ (kw.and ~> prec1) ^^ {
      case lo ~ hi =>
        n => FunApp(Func.BuiltIn(Symbol.Between), List(n, lo, hi), None)
    }

    def `|` : Parser[N => N] = operand.| ~ prec2 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def `||` : Parser[N => N] = operand.|| ~ prec2 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def ^ : Parser[N => N] = operand.^ ~ prec3 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def & : Parser[N => N] = operand.& ~ prec4 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def && : Parser[N => N] = operand.&& ~ prec4 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def > : Parser[N => N] = operand.> ~ prec5 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def >= : Parser[N => N] = operand.>= ~ prec5 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def < : Parser[N => N] = operand.< ~ prec5 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def <= : Parser[N => N] = operand.<= ~ prec5 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def `=` : Parser[N => N] = operand.`=` ~ prec6 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def != : Parser[N => N] = operand.!= ~ prec6 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def unaryMinus: Parser[N => N] = operand.- ^^ { case f =>
      n => FunApp(f, List(n), None)
    }
    def unaryPlus: Parser[N => N] = operand.+ ^^ { case _ => n => n }
    def minus: Parser[N => N] = operand.- ~ prec7 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def plus: Parser[N => N] = operand.+ ~ prec7 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def mult: Parser[N => N] = operand.* ~ prec8 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def / : Parser[N => N] = operand./ ~ prec8 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }
    def % : Parser[N => N] = operand.% ~ prec8 ^^ { case f ~ r =>
      l => FunApp(f, List(l, r), None)
    }

    def prec1: Parser[N] = rep(not) ~ prec2 ^^ { case fs ~ a =>
      fs.foldLeft(a) { (c, i) => i(c) }
    }
    def prec2: Parser[N] = prec3 ~ rep(`|` | `||`) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec3: Parser[N] = prec4 ~ rep(^) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec4: Parser[N] = prec5 ~ rep(& | &&) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec5: Parser[N] = prec6 ~ rep(> | >= | < | <=) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec6: Parser[N] = prec7 ~ rep(`=` | !=) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec7: Parser[N] = prec8 ~ rep(minus | plus) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec8: Parser[N] = rep(unaryPlus | unaryMinus) ~ prec9 ^^ {
      case fs ~ a => fs.foldLeft(a)((c, i) => i(c))
    }
    def prec9: Parser[N] = prec10 ~ rep(mult | / | %) ^^ { case a ~ fs =>
      fs.foldLeft(a)((c, i) => i(c))
    }
    def prec10: Parser[N] =
      cast | aggregateFunction | customFunction | selectCol | embedded.literal | ref | exists | subquery | parens(
        true
      )(expr)

    /** Operator precedence as specified by Scala: 
      (characters not shown below)
      * / %
      + -
      :
      = !
      < >
      &
      ^
      |
      (all letters, $, _)
    */
    def apply(v: Input): ParseResult[N] =
      val prec0 =
        prec1 ~ rep(all | any | nullCheck | and | or | in | like | between) ^^ {
          case a ~ fs => fs.foldLeft(a)((c, i) => i(c))
        }
      prec0(v)

  def apply(src: String): Try[N] =
    parseAll(sql, src) match
      case Success(tree, _) => scala.util.Success(tree)
      case Error(msg, i)    => scala.util.Failure(GraceException(msg))
      case Failure(msg, i)  => scala.util.Failure(GraceException(msg))

object SQLParser:
  object kw:
    val registry: Map[String, Regex] = Map(
      "drop" -> """(?i)drop""".r,
      "delete" -> """(?i)delete""".r,
      "create" -> """(?i)create""".r,
      "table" -> """(?i)table""".r,
      "key" -> """(?i)key""".r,
      "primary" -> """(?i)primary""".r,
      "foreign" -> """(?i)foreign""".r,
      "references" -> """(?i)references""".r,
      "index" -> """(?i)index""".r,
      "auto_increment" -> """(?i)auto_increment""".r,
      "default" -> """(?i)default""".r,
      "cascade" -> """(?i)cascade""".r,
      "restrict" -> """(?i)restrict""".r,
      "set" -> """(?i)set""".r,
      "select" -> """(?i)select""".r,
      "distinct" -> """(?i)distinct""".r,
      "from" -> """(?i)from""".r,
      "as" -> """(?i)as""".r,
      "dual" -> """(?i)dual""".r,
      "star" -> """\*""".r,
      "not" -> """(?i)not""".r,
      "all" -> """(?i)all""".r,
      "any" -> """(?i)any""".r,
      "some" -> """(?i)some""".r,
      "is" -> """(?i)is""".r,
      "null" -> """(?i)null""".r,
      "exists" -> """(?i)exists""".r,
      "and" -> """(?i)and""".r,
      "or" -> """(?i)or""".r,
      "in" -> """(?i)in""".r,
      "like" -> """(?i)like""".r,
      "between" -> """(?i)between""".r,
      "where" -> """(?i)where""".r,
      "by" -> """(?i)by""".r,
      "group" -> """(?i)group""".r,
      "order" -> """(?i)order""".r,
      "offset" -> """(?i)offset""".r,
      "limit" -> """(?i)limit""".r,
      "join" -> """(?i)join""".r,
      "inner" -> """(?i)inner""".r,
      "left" -> """(?i)left""".r,
      "right" -> """(?i)right""".r,
      "full" -> """(?i)full""".r,
      "cross" -> """(?i)cross""".r,
      "on" -> """(?i)on""".r,
      "having" -> """(?i)having""".r,
      "asc" -> """(?i)asc""".r,
      "desc" -> """(?i)desc""".r,
      "," -> """\,""".r,
      "." -> """\.""".r,
      ";" -> """\;""".r,
      "count" -> """(?i)count""".r,
      "sum" -> """(?i)sum""".r,
      "min" -> """(?i)min""".r,
      "max" -> """(?i)max""".r,
      "avg" -> """(?i)avg""".r,
      "first" -> """(?i)first""".r,
      "cast" -> """(?i)cast""".r,
      //unused, but reserved
      "true" -> """(?i)true""".r,
      "false" -> """(?i)false""".r,      
    )
    def drop = registry("drop")
    def delete = registry("delete")
    def create = registry("create")
    def table = registry("table")
    def key = registry("key")
    def primary = registry("primary")
    def foreign = registry("foreign")
    def references = registry("references")
    def index = registry("index")
    def auto_increment = registry("auto_increment")
    def default = registry("default")
    def cascade = registry("cascade")
    def restrict = registry("restrict")
    def set = registry("set")        
    def select = registry("select")
    def distinct = registry("distinct")
    def from = registry("from")
    def as = registry("as")
    def dual = registry("dual")
    def star = registry("star")
    def not = registry("not")
    def all = registry("all")
    def any = registry("any")
    def some = registry("some")
    def is = registry("is")
    def `null` = registry("null")
    def exists = registry("exists")
    def and = registry("and")
    def or = registry("or")
    def in = registry("in")
    def like = registry("like")
    def between = registry("between")
    def where = registry("where")
    def by = registry("by")
    def group = registry("group")
    def order = registry("order")
    def offset = registry("offset")
    def limit = registry("limit")
    def join = registry("join")
    def inner = registry("inner")
    def left = registry("left")
    def right = registry("right")
    def full = registry("full")
    def cross = registry("cross")
    def on = registry("on")
    def having = registry("having")
    def asc = registry("asc")
    def desc = registry("desc")
    def `,` = registry(",")
    def `.` = registry(".")
    def `;` = registry(";")        
    def count = registry("count")        
    def sum = registry("sum")        
    def min = registry("min")        
    def max = registry("max")        
    def avg = registry("avg")
    def first = registry("first")
    def cast = registry("cast")