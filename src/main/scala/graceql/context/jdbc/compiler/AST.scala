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

object Symbol:
  Symbol.values

type Join[L[_], T[_]] = (JoinType, Node[L, T], Node[L, T])
type Ordered[L[_], T[_]] = (Node[L, T], Order)
type GroupBy[L[_], T[_]] = (List[Node[L, T]], Option[Node[L, T]])

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
  case SelectCol[L[_], T[_]](tree: Node[L, T], column: String)
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
              (es.map(_.transform.pre(f)), h.map(_.transform.pre(f)))
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
        case SelectCol(t, c) => SelectCol(t.transform.pre(f), c)
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
              (es.map(_.transform.post(f)), h.map(_.transform.post(f)))
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
        case SelectCol(t, c) => SelectCol(t.transform.post(f), c)
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
              (e.map(_.transform.both(fl, ft)), h.map(_.transform.both(fl, ft)))
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
        case SelectCol(t, c) => SelectCol(t.transform.both(fl, ft), c)
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
  object kw:
    val registry: Map[String, Regex] = Map(
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
      "desc" -> """(?i)desc""".r
    )
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
    def groupBy: Parser[GroupBy[L,T]] = kw.group ~> kw.by ~> tuple(expr) ~ (kw.having ~> expr).? ^^ {case Tuple(es) ~ h => (es, h)}
    def orderBy: Parser[List[Ordered[L,T]]] = 
      val asc = kw.asc ^^ {_ => Order.Asc}
      val desc = kw.desc ^^ {_ => Order.Desc}
      val ord = (asc | desc).? ^^ (_.getOrElse(Order.Asc))
      kw.order ~> kw.by ~> rep1sep(expr ~ ord, ",".r) ^^ {es =>
        es.map{case e ~ o => (e, o)}
      }
    def offset: Parser[N] = kw.offset ~> expr
    def limit: Parser[N] = kw.limit ~> expr
    def selectColumns: Parser[N] =
      tuple(aliased(false)(expr)) | star

    def apply(v: Input): ParseResult[N] =
      def written =
        (kw.select ~> kw.distinct.?) ~ selectColumns ~ (kw.from ~> src) ~ rep(join) ~ where.? ~ groupBy.? ~ orderBy.? ~ limit.? ~ offset.? ^^ {
          case distinct ~ colset ~ from ~ joins ~ where ~ groupBy ~ orderBy ~ limit ~ offset =>
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
  def parens(required: Boolean)(without: => Parser[N]): Parser[N] =
    val `with` = "(" ~> parens(false)(without) <~ ")"
    if required then `with` else `with` | without
  def dual: Parser[N] = embedded.dual | kw.dual ^^ { _ => Dual() }
  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^? ({
    case p if !kw.registry.values.exists(r => r.matches(p)) => p
  }, t => s"Keyword \"${t}\" cannot be used as identifier")
  def ref: Parser[N] =
    embedded.ref | ident ^^ { r => Ref(r) }
  def updateQuery: Parser[N] = ???
  def deleteQuery: Parser[N] = ???
  def insertQuery: Parser[N] = ???
  def createQuery: Parser[N] = ???
  def dropQuery: Parser[N] = ???
  def block: Parser[N] =
    def written = rep1sep(query, ";") ^^ {
      case Nil      => Block(Nil)
      case h +: Nil => h
      case l        => Block(l)
    }
    embedded.block | written
  def tuple(of: => Parser[N]): Parser[N] =
    embedded.tuple | rep1sep(of, ",".r) ^^ { r => Tuple(r) }

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
    selectQuery // | updateQuery | deleteQuery | insertQuery | createQuery | dropQuery
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
    def customFunction: Parser[N] =
      val pat = """([a-zA-Z][a-zA-Z0-9_]*)\(""".r
      pat ~ repsep(expr, ",".r) <~ """\)""".r ^? ({
        case pat(name) ~ args
            if !kw.registry.values.exists(r => r.matches(name)) =>
          (name, args)
      }, { case pat(name) ~ _ =>
        s"Keyword \"${name}\" cannot be used as function name"
      }) ^^ { case (name, args) =>
        FunApp(Func.Custom(name), args, None)
      }
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
      customFunction | embedded.literal | ref | exists | subquery | parens(
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

  def src: Parser[N] =
    aliased(false)(
      embedded.table | subquery | embedded.values
    ) | dual

  def apply(src: String): Try[N] =
    parseAll(sql, src) match
      case Success(tree, _) => scala.util.Success(tree)
      case Error(msg, i)    => scala.util.Failure(GraceException(msg))
      case Failure(msg, i)  => scala.util.Failure(GraceException(msg))
