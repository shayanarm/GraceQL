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
  case Raw[L[_], T[_]](template: StringContext, args: List[Node[L, T]]) extends Node[L, T]
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
      case Raw(qry, args) => args.traverse(_.trans[M](ord)(f)).map(Raw(qry, _))
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
      case Raw(_, args) => args.foldLeft(identity[A])((z,i) => z andThen i.fold(f))
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