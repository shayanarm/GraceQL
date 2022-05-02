package graceql.context.jdbc

import graceql.core.Parser
import scala.util.parsing.combinator._
import scala.util.Try
import graceql.core.GraceException

enum Lit:
  case LChar(value: Char) extends Lit
  case LByte(value: Byte) extends Lit
  case LShort(value: Short) extends Lit
  case LInt(value: Int) extends Lit
  case LLong(value: Long) extends Lit
  case LFloat(value: Float) extends Lit
  case LDouble(value: Double) extends Lit
  case LString(value: String) extends Lit
  case LJSON(value: String) extends Lit

enum JoinType:
  case Inner extends JoinType
  case Left extends JoinType
  case Right extends JoinType
  case Full extends JoinType
  case Cross extends JoinType

enum Order:
  case Asc extends Order
  case Desc extends Order

enum Tree(childern: List[Tree]):
  case Literal(value: Lit) extends Tree(Nil)
  case Select(
      distinct: Boolean,
      columns: Tree,
      from: Tree,
      joins: List[Tree],
      where: Option[Tree],
      groupBy: Option[Tree],
      orderBy: List[Tree],
      offset: Option[Int],
      limit: Option[Int]
  ) extends Tree(
        (from +: joins) ++ where ++ groupBy ++ orderBy
      )
  case Star() extends Tree(Nil)
  case Column(name: String) extends Tree(Nil)
  case ColSet(columns: List[Tree]) extends Tree(columns)
  case Table(table: String) extends Tree(Nil)
  case Values(values: List[Tree]) extends Tree(values)
  case Dual() extends Tree(Nil)
  case As(tree: Tree, alias: String) extends Tree(List(tree))
  case Ref(name: String) extends Tree(Nil)
  case SelectCol(tree: Tree, prop: String) extends Tree(List(tree))
  case Apply(name: String, args: List[Tree]) extends Tree(args)
  case Cast(tpe: String) extends Tree(Nil)
  case Join(joinType: JoinType, tree: Tree, on: Tree)
      extends Tree(List(tree, on))
  case GroupBy(by: List[Tree], having: Option[Tree]) extends Tree(by ++ having)
  case Null() extends Tree(Nil)
  case Union(left: Tree, right: Tree) extends Tree(List(left, right))
  case Ordered(tree: Tree, order: Order) extends Tree(List(tree))
  case Block(stmts: List[Tree]) extends Tree(stmts)

object Tree:
  given Parser[[x] =>> Tree] with

    def apply[A](sc: StringContext)(splices: Any*): Tree =
      val placeholders = splices.indices.map(i => s":$i")
      SQLParser(splices.toArray)(sc.raw(placeholders)).get

class SQLParser(val args: Array[Any]) extends RegexParsers:
  import Tree.*
  object kw:
    def select: Parser[String] = "(?i)select".r ^^ identity
    def distinct: Parser[String] = "(?i)distinct".r ^^ identity
    def from: Parser[String] = "(?i)from".r ^^ identity
    def as: Parser[String] = "(?i)as".r ^^ identity

  def sql: Parser[Tree] = rep1sep(query, ";".r) ^^ { r => Block(r) }
  def query: Parser[Tree] = selectQuery // | update | delete | insert | create | drop
  def selectQuery: Parser[Tree] = kw.select ~ kw.distinct.? ~ selectColumns ~ kw.from ~ src ^^ { case _ ~ distinct ~ colset ~ _ ~ from =>
    Select(distinct.isDefined, colset, from, Nil, None, None, Nil, None, None)
  }
  def selectColumns: Parser[Tree] = tuple(named(expr)) | star
  def tuple(of: Parser[Tree]): Parser[Tree] = rep1sep(of, ",".r) ^^ { r =>
    ColSet(r)
  }
  def named(of: Parser[Tree]): Parser[Tree] = of ~ (kw.as.? ~ ident).? ^^ {
    case o ~ Some(_ ~ i) => As(o, i)
    case o ~ _           => o
  }
  def star: Parser[Tree] = "\\*".r ^^ { _ => Star() }
  def expr: Parser[Tree] = placeholder | named(selectQuery)
  def src: Parser[Tree] = placeholder | named(selectQuery)
  def ident: Parser[String] = "[a-zA-Z][a-ZA-Z0-9_]*".r ^^ identity
  def ref: Parser[Tree] = ident ^^ { r => Ref(r) }
  def placeholder: Parser[Tree] = ":/d".r ^^ { i => replaceWithValue(i.toInt) }
  def update: Parser[Tree] = ???
  def delete: Parser[Tree] = ???
  def insert: Parser[Tree] = ???
  def create: Parser[Tree] = ???
  def drop: Parser[Tree] = ???

  def apply(src: String): Try[Tree] =
    parse(sql, src) match
        case Success(tree, _) => scala.util.Success(tree)
        case Error(msg, i) => scala.util.Failure(GraceException(msg))
        case Failure(msg, i) => scala.util.Failure(GraceException(msg))

  private def replaceWithValue(i: Int): Tree =
    args(i) match
      case v: Char   => Literal(Lit.LChar(v))
      case v: Byte   => Literal(Lit.LByte(v))
      case v: Short  => Literal(Lit.LShort(v))
      case v: Int    => Literal(Lit.LInt(v))
      case v: Long   => Literal(Lit.LLong(v))
      case v: Float  => Literal(Lit.LFloat(v))
      case v: Double => Literal(Lit.LDouble(v))
      case v: String => Literal(Lit.LString(v))
      // case v: JSON => Literal(LJSON(v))
      case v: graceql.context.jdbc.Table[_, _] => Tree.Table(v.name)
