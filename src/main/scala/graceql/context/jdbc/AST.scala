package graceql.context.jdbc

import graceql.core.*
import scala.util.parsing.combinator._
import scala.util.Try
import graceql.core.GraceException
import scala.quoted.Type
import scala.quoted.Expr

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
  case Apply[L[_], T[_], A](name: String, args: List[Node[L, T]], returnType: Type[A]) extends Node[L, T](args)
  case Cast[L[_], T[_], A](tree: Node[L, T], tpe: Type[A]) extends Node[L, T](Nil)
  case Join[L[_], T[_]](joinType: JoinType, tree: Node[L, T], on: Node[L, T])
      extends Node[L, T](List(tree, on))
  case GroupBy[L[_], T[_]](by: List[Node[L, T]], having: Option[Node[L, T]]) extends Node[L, T](by ++ having)
  case Null() extends Node[L, T](Nil)
  case Union[L[_], T[_]](left: Node[L, T], right: Node[L, T]) extends Node[L, T](List(left, right))
  case Ordered[L[_], T[_]](tree: Node[L, T], order: Order) extends Node[L, T](List(tree))
  case Block[L[_], T[_]](stmts: List[Node[L, T]]) extends Node[L, T](stmts)

type Tree = Node[Encoded, [x] =>> Unit]  
object Node:
  given defualt[L[_],T[_]]:NativeSupport[[x] =>> Node[L, T]] with {}

class SQLParser[L[_],T[_]](val args: Array[Node[L, T]]) extends RegexParsers:
  import Node.*
  object kw:
    def select: Parser[String] = """(?i)select""".r ^^ identity
    def distinct: Parser[String] = """(?i)distinct""".r ^^ identity
    def from: Parser[String] = """(?i)from""".r ^^ identity
    def as: Parser[String] = """(?i)as""".r ^^ identity
    def dual: Parser[String] = """(?i)dual""".r ^^ identity

  def sql: Parser[Node[L, T]] = rep1sep(query, ";".r) ^^ { r => Block(r) }
  def query: Parser[Node[L, T]] = selectQuery // | update | delete | insert | create | drop
  def selectQuery: Parser[Node[L, T]] = (kw.select ~> kw.distinct.?) ~ selectColumns ~ (kw.from ~> src) ^^ { case distinct ~ colset ~ from =>
    Select(distinct.isDefined, colset, from, Nil, None, None, Nil, None, None)
  }
  def selectColumns: Parser[Node[L, T]] = tuple(aliased(expr)) | star
  def tuple(of: Parser[Node[L, T]]): Parser[Node[L, T]] = rep1sep(of, ",".r) ^^ { r =>
    Tuple(r)
  }
  def aliased(of: Parser[Node[L, T]]): Parser[Node[L, T]] = of ~ (kw.as.? ~> ident).? ^^ {
    case o ~ Some(i) => As(o, i)
    case o ~ _           => o
  }
  def star: Parser[Node[L, T]] = """\*""".r ^^ { _ => Star() }
  def expr: Parser[Node[L, T]] = aliased(placeholder | selectQuery)
  def src: Parser[Node[L, T]] = aliased(table | selectQuery) | dual
  def dual: Parser[Node[L, T]] = kw.dual ^^ {_ => Dual()}
  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^^ identity
  def ref: Parser[Node[L, T]] = ident ^^ { r => Ref(r) }
  def placeholder: Parser[Node[L, T]] = (""":""".r ~> """\d""".r) ^^ { i => args(i.toInt) }
  def table: Parser[Node[L, T]] = placeholder ^? ({case t: Table[_,_,_] => t}, t => s"Expected a \"Table\" reference, but found $t")
  def update: Parser[Node[L, T]] = ???
  def delete: Parser[Node[L, T]] = ???
  def insert: Parser[Node[L, T]] = ???
  def create: Parser[Node[L, T]] = ???
  def drop: Parser[Node[L, T]] = ???

  def apply(src: String): Try[Node[L, T]] =
    parse(sql, src) match
        case Success(tree, _) => scala.util.Success(tree)
        case Error(msg, i) => scala.util.Failure(GraceException(msg))
        case Failure(msg, i) => scala.util.Failure(GraceException(msg))
