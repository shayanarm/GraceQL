package graceql.context.jdbc

import graceql.core.*
import scala.util.parsing.combinator._
import scala.util.Try
import graceql.core.GraceException
import scala.util.matching.Regex

case class Encoded[+A](val original: A, val run: () => String):
  override def toString() = run()
  override def equals(other: Any): Boolean =
    other match
      case Encoded(that,_) => original == that
      case _ => super.equals(other)

enum JoinType:
  case Inner extends JoinType
  case Left extends JoinType
  case Right extends JoinType
  case Full extends JoinType
  case Cross extends JoinType

enum Order:
  case Asc extends Order
  case Desc extends Order

enum Node[L[_], T[_]](childern: List[Node[L, T]]):
  case Literal[L[_], T[_], A](value: L[A]) extends Node[L, T](Nil)
  case Select[L[_], T[_]](
      distinct: Boolean,
      columns: Node[L, T],
      from: Node[L, T],
      joins: List[Node[L, T]],
      where: Option[Node[L, T]],
      groupBy: Option[Node[L, T]],
      orderBy: List[Node[L, T]],
      offset: Option[L[Int]],
      limit: Option[L[Int]]
  ) extends Node[L, T](
        (from +: joins) ++ where ++ groupBy ++ orderBy
      )
  case Star() extends Node[L, T](Nil)
  case Column(name: String) extends Node[L, T](Nil)
  case Tuple[L[_], T[_]](columns: List[Node[L, T]]) extends Node[L, T](columns)
  case Table[L[_], T[_], A](table: L[String], tpe: T[A]) extends Node[L, T](Nil)
  case Values[L[_], T[_], A](values: L[Iterable[A]]) extends Node[L, T](Nil)
  case Dual() extends Node[L, T](Nil)
  case As[L[_], T[_]](tree: Node[L, T], alias: String) extends Node[L, T](List(tree))
  case Ref(name: String) extends Node[L, T](Nil)
  case SelectCol[L[_], T[_]](tree: Node[L, T], column: String) extends Node[L, T](List(tree))
  case Apply[L[_], T[_], A](name: String, args: List[Node[L, T]], returnType: T[A]) extends Node[L, T](args)
  case Cast[L[_], T[_], A](tree: Node[L, T], tpe: T[A]) extends Node[L, T](Nil)
  case Join[L[_], T[_]](joinType: JoinType, tree: Node[L, T], on: Node[L, T])
      extends Node[L, T](List(tree, on))
  case GroupBy[L[_], T[_]](by: List[Node[L, T]], having: Option[Node[L, T]]) extends Node[L, T](by ++ having)
  case Null() extends Node[L, T](Nil)
  case Union[L[_], T[_]](left: Node[L, T], right: Node[L, T]) extends Node[L, T](List(left, right))
  case Ordered[L[_], T[_]](tree: Node[L, T], order: Order) extends Node[L, T](List(tree))
  case Block[L[_], T[_]](stmts: List[Node[L, T]]) extends Node[L, T](stmts)

  def printer(using eq: Node[L,T] =:= Node[Encoded,T]) = Printer(eq(this))

class Printer(val tree: Node[Encoded, _]):
  import Node.*
  private def compactRec(q: Node[Encoded, _]): String = 
    q match
      case Select(distinct, columns, from, joins, where, groupBy, orderBy, offset, limit) =>
        val builder = StringBuilder()
        builder.append("SELECT ")
        if distinct then
          builder.append("DISTINCT ")
        builder.append(compactRec(columns) + " ")
        builder.append(s"FROM ${compactRec(from)}")
        // s"""
        // SELECT ${if distinct then "DISTINCT " else ""}${compactRec(columns)}
        // FROM ${compactRec(from)}
        // ${joins.map(compactRec).mkString("\n")}
        // ${where.fold("")(i => s"WHERE ${compactRec(i)}")}
        // ${groupBy.fold("")(i => s"GROUP BY ${compactRec(i)}")}
        // """
        builder.toString
      case Block(stmts) => stmts.map(compactRec).mkString(";")
      case Star() => "*"
      case Tuple(trees) => trees.map(compactRec).mkString(", ")
      case As(tree, name) => s"${compactRec(tree)} AS ${name}"
      case Table(name, _) => s"${name.run()}"
      case Literal(encoder) => encoder.run()
  def compact: String = compactRec(tree)

type Tree = Node[Encoded, [x] =>> Unit]

class SQLParser[L[_],T[_]](val args: Array[Node[L, T]]) extends RegexParsers:
  import Node.*
  object kw:
    def registry: Map[String, Regex] = Map(
      "select" -> """(?i)select""".r,
      "distinct" -> """(?i)distinct""".r,
      "from" -> """(?i)from""".r,
      "as" -> """(?i)as""".r,
      "dual" -> """(?i)dual""".r,
      "star" -> """\*""".r,
      "semicolon" -> ";".r
    )
    def select = registry("select")
    def distinct = registry("distinct")
    def from = registry("from")
    def as = registry("as")
    def dual = registry("dual")
    def star = registry("star")
    def semicolon = registry("semicolon")

  def sql: Parser[Node[L, T]] = rep1sep(query, kw.semicolon) ^^ { r => Block(r) }
  def query: Parser[Node[L, T]] = selectQuery // | updateQuery | deleteQuery | insertQuery | createQuery | dropQuery
  def selectQuery: Parser[Node[L, T]] = (kw.select ~> kw.distinct.?) ~ selectColumns ~ (kw.from ~> src) ^^ { case distinct ~ colset ~ from =>
    Select(distinct.isDefined, colset, from, Nil, None, None, Nil, None, None)
  }
  def selectColumns: Parser[Node[L, T]] = tuple(aliased(expr)) | star
  def tuple(of: Parser[Node[L, T]]): Parser[Node[L, T]] = 
    rep1sep(of, ",".r) ^^ { r => Tuple(r) }
  def aliased(of: Parser[Node[L, T]]): Parser[Node[L, T]] = of ~ (kw.as.? ~> ident).? ^^ {
    case o ~ Some(i) => As(o, i)
    case o ~ _           => o
  }
  def star: Parser[Node[L, T]] = kw.star ^^ { _ => Star() }
  def expr: Parser[Node[L, T]] = literal | selectQuery
  def src: Parser[Node[L, T]] = aliased(table | selectQuery) | dual
  def dual: Parser[Node[L, T]] = kw.dual ^^ {_ => Dual()}
  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^? ({
    case p if !kw.registry.values.exists(r => r.matches(p)) => p
  }, t => s"Keyword \"${t}\" cannot be used as identifier")
  def ref: Parser[Node[L, T]] = ident ^^ { r => Ref(r) }
  def placeholder: Parser[Node[L, T]] = (""":""".r ~> """\d+""".r) ^^ { i => 
    args(i.toInt) 
  }
  def table: Parser[Node[L, T]] = placeholder ^? ({case t: Table[_,_,_] => t}, t => s"Expected a \"Table\" reference, but found $t")
  def literal: Parser[Node[L, T]] = placeholder ^? ({case t: Literal[_,_,_] => t}, t => s"Expected a \"Literal\" reference, but found $t")
  def updateQuery: Parser[Node[L, T]] = ???
  def deleteQuery: Parser[Node[L, T]] = ???
  def insertQuery: Parser[Node[L, T]] = ???
  def createQuery: Parser[Node[L, T]] = ???
  def dropQuery: Parser[Node[L, T]] = ???

  def apply(src: String): Try[Node[L, T]] =
    parse(sql, src) match
        case Success(tree, _) => scala.util.Success(tree)
        case Error(msg, i) => scala.util.Failure(GraceException(msg))
        case Failure(msg, i) => scala.util.Failure(GraceException(msg))
