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
      offset: Option[Node[L, T]],
      limit: Option[Node[L, T]]
  ) extends Node[L, T](
        List(
          columns,
          from
        ) ++ joins ++ where ++ groupBy ++ orderBy ++ offset ++ limit
      )
  case Star() extends Node[L, T](Nil)
  case Column(name: String) extends Node[L, T](Nil)
  case Tuple[L[_], T[_]](columns: List[Node[L, T]]) extends Node[L, T](columns)
  case Table[L[_], T[_], A](table: L[String], tpe: T[A]) extends Node[L, T](Nil)
  case Values[L[_], T[_], A](values: L[Iterable[A]]) extends Node[L, T](Nil)
  case Dual() extends Node[L, T](Nil)
  case As[L[_], T[_]](tree: Node[L, T], alias: String)
      extends Node[L, T](List(tree))
  case Ref(name: String) extends Node[L, T](Nil)
  case SelectCol[L[_], T[_]](tree: Node[L, T], column: String)
      extends Node[L, T](List(tree))
  case FunApp[L[_], T[_], A](
      name: String,
      args: List[Node[L, T]],
      returnType: T[A]
  ) extends Node[L, T](args)
  case Operator[L[_], T[_], A](
      symbol: String,
      left: Node[L, T],
      right: Node[L, T],
      returnType: T[A]
  ) extends Node[L, T](List(left, right))
  case Cast[L[_], T[_], A](tree: Node[L, T], tpe: T[A]) extends Node[L, T](Nil)
  case Join[L[_], T[_]](joinType: JoinType, tree: Node[L, T], on: Node[L, T])
      extends Node[L, T](List(tree, on))
  case GroupBy[L[_], T[_]](by: List[Node[L, T]], having: Option[Node[L, T]])
      extends Node[L, T](by ++ having)
  case Null() extends Node[L, T](Nil)
  case Union[L[_], T[_]](left: Node[L, T], right: Node[L, T])
      extends Node[L, T](List(left, right))
  case Ordered[L[_], T[_]](tree: Node[L, T], order: Order)
      extends Node[L, T](List(tree))
  case Block[L[_], T[_]](stmts: List[Node[L, T]]) extends Node[L, T](stmts)

type Tree = Node[[x] =>> String, [x] =>> Unit]

class SQLParser[L[_], T[_]](val args: Array[Node[L, T]]) extends RegexParsers:
  private type N = Node[L, T]
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

  def placeholder: Parser[N] = (""":""".r ~> """\d+""".r) ^^ { i =>
    args(i.toInt)
  }
  def embeddable[A](
      f: PartialFunction[Node[L, T], A],
      msg: Node[L, T] => String = n =>
        s"Embedded Tree is not acceptable in this position:\n${n}"
  ) =
    placeholder ^? (f, msg)
  def sql: Parser[N] = block | query | expr
  def block: Parser[N] = rep1sep(query, kw.semicolon) ^^ {
    case Nil      => Block(Nil)
    case h +: Nil => h
    case l        => Block(l)
  }
  def query: Parser[N] =
    selectQuery // | updateQuery | deleteQuery | insertQuery | createQuery | dropQuery
  def selectQuery: Parser[N] =
    embeddable { case t: Select[_, _] =>
      t
    } |
      ((kw.select ~> kw.distinct.?) ~ selectColumns ~ (kw.from ~> src) ~ selectClauses ^^ {
        case distinct ~ colset ~ from ~ (joins, where, groupBy, orderBy, offset,
            limit) =>
          Select(
            distinct.isDefined,
            colset,
            from,
            joins,
            where,
            groupBy,
            orderBy,
            offset,
            limit
          )
      })
  def selectColumns: Parser[N] = tuple(aliased(expr)) | star
  def selectClauses
      : Parser[(List[N], Option[N], Option[N], List[N], Option[N], Option[N])] =
    success((Nil, None, None, Nil, None, None))
  def tuple(of: Parser[N]): Parser[N] =
    rep1sep(of, ",".r) ^^ { r => Tuple(r) }
  def aliased(
      of: Parser[N],
      required: Boolean = false
  ): Parser[N] =
    val alias = kw.as.? ~> ident
    required match
      case false =>
        of ~ alias.? ^^ {
          case o ~ Some(i) => As(o, i)
          case o ~ _       => o
        }
      case true => of ~ alias ^^ { case o ~ i => As(o, i) }
  def star: Parser[N] = kw.star ^^ { _ => Star() }
  def expr: Parser[N] = literal | selectQuery
  def src: Parser[N] = aliased(table | selectQuery) | dual
  def dual: Parser[N] = kw.dual ^^ { _ => Dual() }
  def ident: Parser[String] = """[a-zA-Z][a-zA-Z0-9_]*""".r ^? ({
    case p if !kw.registry.values.exists(r => r.matches(p)) => p
  }, t => s"Keyword \"${t}\" cannot be used as identifier")
  def ref: Parser[N] = ident ^^ { r => Ref(r) }

  def table: Parser[N] = embeddable(
    { case t: Table[_, _, _] =>
      t
    },
    t => s"Expected a \"Table\" reference, but found $t"
  )
  def literal: Parser[N] = embeddable(
    { case t: Literal[_, _, _] =>
      t
    },
    t => s"Expected a \"Literal\" reference, but found $t"
  )
  def updateQuery: Parser[N] = ???
  def deleteQuery: Parser[N] = ???
  def insertQuery: Parser[N] = ???
  def createQuery: Parser[N] = ???
  def dropQuery: Parser[N] = ???

  def apply(src: String): Try[N] =
    parse(sql, src) match
      case Success(tree, _) => scala.util.Success(tree)
      case Error(msg, i)    => scala.util.Failure(GraceException(msg))
      case Failure(msg, i)  => scala.util.Failure(GraceException(msg))
