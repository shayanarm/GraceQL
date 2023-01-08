package graceql.context.jdbc.compiler

import graceql.core.*
import scala.util.parsing.combinator._
import scala.util.Try
import graceql.core.GraceException
import graceql.data.{Id, Monad, ~}
import scala.util.matching.Regex
import scala.quoted.*
import graceql.syntax.*

enum JoinType:
  case Inner extends JoinType
  case Left extends JoinType
  case Right extends JoinType
  case Full extends JoinType
  case Cross extends JoinType

enum Order:
  case Asc extends Order
  case Desc extends Order

object Order:
  given FromExpr[Order] with
    def unapply(expr: Expr[Order])(using q: Quotes): Option[Order] = 
      import q.reflect.*
      expr match
        case '{Asc} => Some(Asc)
        case '{Desc} => Some(Desc)
        case _ => None  

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
  case Uniques[L[_], T[_]](indices: List[String]) extends CreateSpec[L,T]

enum ColMod[L[_]]:
  case NotNull[L[_]]() extends ColMod[L]
  case AutoInc[L[_]]() extends ColMod[L]
  case Default[L[_], A](value: L[A]) extends ColMod[L]
  case Unique[L[_]]() extends ColMod[L]

enum OnDelete:
  case Cascade extends OnDelete
  case SetDefault extends OnDelete
  case SetNull extends OnDelete
  case Restrict extends OnDelete

object OnDelete:
  given FromExpr[OnDelete] with
    def unapply(expr: Expr[OnDelete])(using q: Quotes): Option[OnDelete] = 
      import q.reflect.*
      expr match
        case '{Cascade} => Some(Cascade)
        case '{SetDefault} => Some(SetDefault)
        case '{SetNull} => Some(SetNull)
        case '{Restrict} => Some(Restrict)
        case _ => None     


enum Node[L[_], T[_]]:
  self =>
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
  case DropTable[L[_], T[_]](table: Node[L, T]) extends Node[L, T]
  case CreateTable[L[_], T[_]](table: Node[L, T], specs: Option[List[CreateSpec[L,T]]]) extends Node[L, T]
  

  private def trans[M[_]](ord: Node.Traversal)(f: PartialFunction[Node[L, T], M[Node[L, T]]])(using m: Monad[M]): M[Node[L, T]] =
    val g = f.orElse { case t => m.pure(t) }
    val k: Node[L, T] => M[Node[L, T]] = node => node match
      case Literal(_) => node.pure[M]
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
            columns.trans[M](ord)(f)
              ~ from.trans[M](ord)(f)
              ~ joins.traverse { case (jt, s, o) =>
                s.trans[M](ord)(f).zip(o.trans[M](ord)(f)).map((a,b) => (jt, a, b))
              }
              ~ where.traverse(_.trans[M](ord)(f))
              ~ groupBy.traverse { case (es, h) =>
                es.trans[M](ord)(f).zip(h.traverse(_.trans[M](ord)(f)))
              }
              ~ orderBy.traverse { case (t, o) => t.trans[M](ord)(f).map((_, o)) }
              ~ offset.traverse(_.trans[M](ord)(f))
              ~ limit.traverse(_.trans[M](ord)(f))
              ^^ { case a ~ b ~ c ~ d ~ e ~ f ~ g ~ h =>
                Select(distinct, a, b, c, d, e, f, g, h)
              }
      case Star()          => node.pure[M]
      case Column(_)       => node.pure[M]
      case Tuple(elems)    => elems.traverse(_.trans[M](ord)(f)).map(Tuple(_))
      case Table(_, _)     => node.pure[M]
      case Values(_)       => node.pure[M]
      case As(t, n)        => t.trans[M](ord)(f).map(As(_, n))
      case Ref(_)          => node.pure[M]
      case SelectCol(t, c) => t.trans[M](ord)(f) ~ c.trans[M](ord)(f) ^^ (SelectCol(_,_))
      case FunApp(n, args, tpe) =>
        args.traverse(_.trans[M](ord)(f)).map(FunApp(n, _, tpe))
      case TypeLit(_)      => node.pure[M]
      case Cast(t, tpe)    => t.trans[M](ord)(f).map(Cast(_, tpe))
      case TypeAnn(t, tpe) => t.trans[M](ord)(f).map(TypeAnn(_, tpe))
      case Null()          => node.pure[M]
      case Any(c, o, s) =>
        c.trans[M](ord)(f) ~ s.trans[M](ord)(f) ^^ (Any(_, o ,_))
      case All(c, o, s) =>
        c.trans[M](ord)(f) ~ s.trans[M](ord)(f) ^^ (All(_, o, _))        
      case Union(l, r) =>
        l.trans[M](ord)(f) ~ r.trans[M](ord)(f) ^^ (Union(_,_))
      case Block(stmts) => stmts.traverse(_.trans[M](ord)(f)).map(Block(_))
      case DropTable(t) => t.trans[M](ord)(f).map(DropTable(_))
      case CreateTable(t, s) => t.trans[M](ord)(f).map(CreateTable(_, s))

    ord match
      case Node.Traversal.Pre => g(this).flatMap(k)      
      case Node.Traversal.Post => k(this).flatMap(g)
  
  private inline def fold[A](f: A => PartialFunction[Node[L, T], A]): A => A = 
      initial => self.fold[A](initial)(f)

  def fold[A](initial: A)(f: A => PartialFunction[Node[L, T], A]): A =
    inline def composed: A => A = this match
      case Literal(_) => identity
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
        columns.fold(f) andThen 
          from.fold(f) andThen
          joins.foldLeft(identity[A]){case (z, (jt, s, o)) => 
            z andThen s.fold(f) andThen o.fold(f)
          } andThen
          where.foldLeft(identity[A]){(z,i) => 
            z andThen i.fold(f)
          } andThen
          groupBy.foldLeft(identity[A]){case (z, (es, h)) => 
            z andThen es.fold(f) andThen h.foldLeft(identity[A]){(z2, i) => 
              z2 andThen i.fold(f)
            }
          } andThen
          orderBy.foldLeft(identity[A]){case (z, (t, o)) => 
            z andThen t.fold(f)
          } andThen 
          offset.foldLeft(identity[A])((z,i) => z andThen i.fold(f)) andThen
          limit.foldLeft(identity[A])((z,i) => z andThen i.fold(f))
      case Star()          => identity
      case Column(_)       => identity
      case Tuple(elems)    => elems.foldLeft(identity[A])((z,i) => z andThen i.fold(f))
      case Table(_, _)     => identity
      case Values(_)       => identity
      case As(t, n)        => t.fold(f)
      case Ref(_)          => identity
      case SelectCol(t, c) => t.fold(f) andThen c.fold(f)
      case FunApp(n, args, tpe) => args.foldLeft(identity[A])((z,i) => z andThen i.fold(f)) 
      case TypeLit(_)      => identity
      case Cast(t, tpe)    => t.fold(f)
      case TypeAnn(t, tpe) => t.fold(f)
      case Null()          => identity
      case Any(c, o, s) => c.fold(f) andThen s.fold(f)
      case All(c, o, s) => c.fold(f) andThen s.fold(f)
      case Union(l, r) => l.fold(f) andThen r.fold(f)
      case Block(stmts) => stmts.foldLeft(identity[A])((z,i) => z andThen i.fold(f))
      case DropTable(t) => t.fold(f)
      case CreateTable(t, s) => t.fold(f)
    val g = f(initial).orElse { case _ => initial }
    (g andThen composed)(this)

  inline def lits[L2[_]](f: [A] => L[A] => L2[A]): Node[L2, T] =
    both(f, [x] => (i: T[x]) => i)
  inline def types[T2[_]](f: [A] => T[A] => T2[A]): Node[L, T2] =
    both([x] => (i: L[x]) => i, f)
  protected def both[L2[_], T2[_]](
      fl: [X] => L[X] => L2[X],
      ft: [Y] => T[Y] => T2[Y]
  ): Node[L2, T2] =
    this match
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
          columns.both(fl, ft),
          from.both(fl, ft),
          joins.map { case (jt, s, on) =>
            (jt, s.both(fl, ft), on.both(fl, ft))
          },
          where.map(_.both(fl, ft)),
          groupBy.map { case (e, h) =>
            (e.both(fl, ft), h.map(_.both(fl, ft)))
          },
          orderBy.map { case (t, o) => (t.both(fl, ft), o) },
          offset.map(_.both(fl, ft)),
          limit.map(_.both(fl, ft))
        )
      case Star()          => Star()
      case Column(n)       => Column(n)
      case Tuple(elems)    => Tuple(elems.map(_.both(fl, ft)))
      case Table(n, t)     => Table(fl(n), ft(t))
      case Values(ls)      => Values(fl(ls))
      case As(t, n)        => As(t.both(fl, ft), n)
      case Ref(n)          => Ref(n)
      case SelectCol(t, c) => SelectCol(t.both(fl, ft), c.both(fl, ft))
      case FunApp(n, args, tpe) =>
        FunApp(
          n,
          args.map(_.both(fl, ft)),
          tpe.map(i => ft(i))
        )
      case TypeLit(tpe)    => TypeLit(ft(tpe))
      case Cast(t, tpe)    => Cast(t.both(fl, ft), ft(tpe))
      case TypeAnn(t, tpe) => TypeAnn(t.both(fl, ft), ft(tpe))
      case Null()          => Null()
      case Any(c, o, s) =>
        Any(
          c.both(fl, ft),
          o,
          s.both(fl, ft)
        )
      case All(c, o, s) =>
        All(
          c.both(fl, ft),
          o,
          s.both(fl, ft)
        )
      case Union(l, r) =>
        Union(
          l.both(fl, ft),
          r.both(fl, ft)
        )
      case Block(stmts) => Block(stmts.map(_.both(fl, ft)))
      case DropTable(t) => DropTable(t.both(fl, ft))
      case CreateTable(t,specs) => CreateTable(t.both(fl, ft), specs.map(_.map {
        case CreateSpec.ColDef(name, tpe, mods) =>
          CreateSpec.ColDef(name, ft(tpe), mods.map{
            case ColMod.NotNull() => ColMod.NotNull()
            case ColMod.AutoInc() => ColMod.AutoInc()
            case ColMod.Default(v) => ColMod.Default(fl(v))
            case ColMod.Unique() => ColMod.Unique()
          })            
        case CreateSpec.PK(ks) => CreateSpec.PK(ks)
        case CreateSpec.FK(l, tname, r, od) => CreateSpec.FK(l, fl(tname), r, od)
        case CreateSpec.Index(is) => CreateSpec.Index(is)
        case CreateSpec.Uniques(is) => CreateSpec.Uniques(is)
      }))

  object transform:
    inline def pre[M](f: PartialFunction[Node[L, T], Node[L, T]]): Node[L, T] = 
      preM[Id](f.andThen(Id(_))).unwrap
    inline def preM[M[_] : Monad](f: PartialFunction[Node[L, T], M[Node[L, T]]]): M[Node[L, T]] = 
      self.trans[M](Node.Traversal.Pre)(f)      
    inline def post[M](f: PartialFunction[Node[L, T], Node[L, T]]): Node[L, T] = 
      postM[Id](f.andThen(Id(_))).unwrap
    inline def postM[M[_] : Monad](f: PartialFunction[Node[L, T], M[Node[L, T]]]): M[Node[L, T]] = 
      self.trans[M](Node.Traversal.Post)(f)
    inline def lits[L2[_]](f: [A] => L[A] => L2[A]): Node[L2, T] = self.lits(f)
    inline def types[T2[_]](f: [A] => T[A] => T2[A]): Node[L, T2] = self.types(f)
    inline def both[L2[_], T2[_]](
          fl: [X] => L[X] => L2[X],
          ft: [Y] => T[Y] => T2[Y]
      ): Node[L2, T2] = self.both(fl, ft)        

object Node:
  enum Traversal:
    case Pre
    case Post

  inline def parse[L[_], T[_]](sc: StringContext)(
      args: Seq[Node[L, T]]
  ): Try[Node[L, T]] =
    val placeholders = args.indices.map(i => s":$i")
    val raw = sc.raw(placeholders*)
    parser(args.toArray).apply(raw)
  extension (sc: StringContext)
    inline def gensql[L[_], T[_]](args: Node[L, T]*): Node[L, T] =
      parse(sc)(args).get

  inline def parser[L[_], T[_]](args: Array[Node[L, T]]): SqlParser[L, T] =
    SqlParser[L, T](args)

class SqlParser[L[_], T[_]](val args: Array[Node[L, T]]) extends RegexParsers:
  private type N = Node[L, T]
  import Node.*
  import SqlParser.*
            
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
    )    
  def parens[A](required: Boolean)(without: => Parser[A]): Parser[A] =
    val `with` = "(" ~> parens(false)(without) <~ ")"
    if required then `with` else `with` | without

  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^? ({
    case p if !kw.values.exists(r => r.matches(p)) => p
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

    def unique: Parser[CreateSpec[L,T]] = kw.unique ~> parens(true)(rep1sep(expr.column, kw.`,`)) ^^ {is => 
      CreateSpec.Uniques(is.map{case Column(c) => c})
    }

    def columnModifier: Parser[ColMod[L]] = 
      def autoInc: Parser[ColMod[L]] = kw.auto_increment ^^ {_ => ColMod.AutoInc()}
      def notNull: Parser[ColMod[L]] = kw.not ~> kw.`null` ^^ {_ => ColMod.NotNull()}
      def default: Parser[ColMod[L]] = kw.default ~> embedded.literal ^^ {case Literal(l) => ColMod.Default(l)}
      def unique: Parser[ColMod[L]] = kw.unique ^^ {_ => ColMod.Unique()}
      autoInc | notNull | default | unique

    def columnSpec: Parser[CreateSpec[L,T]] = fk | pk | index | unique | columnDef
    def apply(v: Input): ParseResult[N] =
      // def specs: Parser[CreateSpec[L,T]] = parens(true)(rep1sep(columnSpec, kw.`,`))
      def parser: Parser[N] = kw.create ~> kw.table ~> embedded.table /*~ specs*/ ^^ {case t => CreateTable(t, None)}
      parser(v)

  def dropQuery: Parser[N] = kw.drop ~> kw.table ~> embedded.table ^^ {case t => DropTable(t)}
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

object SqlParser:
  enum kw(regex: String) extends Regex(regex):
    case drop extends kw("""(?i)drop""")
    case delete extends kw("""(?i)delete""")
    case create extends kw("""(?i)create""")
    case table extends kw("""(?i)table""")
    case key extends kw("""(?i)key""")
    case primary extends kw("""(?i)primary""")
    case foreign extends kw("""(?i)foreign""")
    case references extends kw("""(?i)references""")
    case index extends kw("""(?i)index""")
    case unique extends kw("""(?i)unique""")
    case auto_increment extends kw("""(?i)auto_increment""")
    case default extends kw("""(?i)default""")
    case cascade extends kw("""(?i)cascade""")
    case restrict extends kw("""(?i)restrict""")
    case set extends kw("""(?i)set""")
    case select extends kw("""(?i)select""")
    case distinct extends kw("""(?i)distinct""")
    case from extends kw("""(?i)from""")
    case as extends kw("""(?i)as""")
    case star extends kw("""\*""")
    case not extends kw("""(?i)not""")
    case all extends kw("""(?i)all""")
    case any extends kw("""(?i)any""")
    case some extends kw("""(?i)some""")
    case is extends kw("""(?i)is""")
    case `null` extends kw("""(?i)null""")
    case exists extends kw("""(?i)exists""")
    case and extends kw("""(?i)and""")
    case or extends kw("""(?i)or""")
    case in extends kw("""(?i)in""")
    case like extends kw("""(?i)like""")
    case between extends kw("""(?i)between""")
    case where extends kw("""(?i)where""")
    case by extends kw("""(?i)by""")
    case group extends kw("""(?i)group""")
    case order extends kw("""(?i)order""")
    case offset extends kw("""(?i)offset""")
    case limit extends kw("""(?i)limit""")
    case join extends kw("""(?i)join""")
    case inner extends kw("""(?i)inner""")
    case left extends kw("""(?i)left""")
    case right extends kw("""(?i)right""")
    case full extends kw("""(?i)full""")
    case cross extends kw("""(?i)cross""")
    case on extends kw("""(?i)on""")
    case having extends kw("""(?i)having""")
    case asc extends kw("""(?i)asc""")
    case desc extends kw("""(?i)desc""")
    case `,` extends kw("""\,""")
    case `.` extends kw("""\.""")
    case `;` extends kw("""\;""")
    case count extends kw("""(?i)count""")
    case sum extends kw("""(?i)sum""")
    case min extends kw("""(?i)min""")
    case max extends kw("""(?i)max""")
    case avg extends kw("""(?i)avg""")
    case first extends kw("""(?i)first""")
    case cast extends kw("""(?i)cast""")
    //unused, but reserved
    case `true` extends kw("""(?i)true""")
    case `false` extends kw("""(?i)false""")