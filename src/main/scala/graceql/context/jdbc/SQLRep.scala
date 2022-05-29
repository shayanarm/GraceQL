package graceql.context.jdbc

import scala.deriving.*
import scala.compiletime.*
import scala.collection.IterableFactory
import scala.concurrent.duration.fromNow
import graceql.core.GraceException

sealed trait Row
sealed trait Base
sealed trait Mapped[A]
sealed trait ValueClass
sealed trait JsonADT
sealed trait JsonArray

type Column = Base | Mapped[_ <: Any] | ValueClass | JsonADT | JsonArray
type Reps = Row | Column

sealed trait SQLRep[A]:
  type Rep <: Reps
  def arity: Int
  def fromRow(r: List[Option[Any]]): A
  def toRow(a: A): List[Option[Any]]

object SQLRep:
  given option[A](using ta: SQLRep[A]): SQLRep[Option[A]] with
    final type Rep = ta.Rep
    def arity: Int = ta.arity
    def fromRow(r: List[Option[Any]]): Option[A] =
      if r.forall(_.isEmpty) then None else Some(ta.fromRow(r))
    def toRow(a: Option[A]): List[Option[Any]] =
      a.fold(List.fill(arity)(None))(ta.toRow)

  given jsonArraySeq[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[Seq, A] = SQLJsonArray(Seq)
  given jsonArrayIndexedSeq[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[IndexedSeq, A] = SQLJsonArray(IndexedSeq)
  given jsonArrayList[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[List, A] = SQLJsonArray(List)
  given jsonArrayVector[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[Vector, A] = SQLJsonArray(Vector)
  given jsonArrayLazyList[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[LazyList, A] = SQLJsonArray(LazyList)
  given jsonArrayIterable[A](using ta: SQLRep[A])(using ev: ta.Rep <:< Column): SQLJsonArray[Iterable, A] = SQLJsonArray(Iterable)      

  given BaseSQL[Int] with {}
  given BaseSQL[String] with {}

sealed trait BaseSQL[A] extends SQLRep[A]:
  final type Rep = Base

  def arity = 1

  final def fromRow(r: List[Option[Any]]): A =
    r.head.get.asInstanceOf[A]

  final def toRow(m: A): List[Option[Any]] = List(Some(m))

trait SQLMapped[A, B](using val ta: SQLRep[A])(using ev: ta.Rep <:< Column)
    extends SQLRep[B]:
  final type Rep = Mapped[A]

  def fromA(a: A): B

  def toA(b: B): A

  def arity = 1
  final def fromRow(r: List[Option[Any]]): B = fromA(
    ta.fromRow(r)
  )
  final def toRow(b: B): List[Option[Any]] = ta.toRow(toA(b))

object SQLMapped:
  def apply[A, B](_from: A => B, _to: B => A)(using ta: SQLRep[A])(using
      ev: ta.Rep <:< Column
  ) =
    new SQLMapped[A, B]:
      def fromA(a: A): B = _from(a)
      def toA(b: B): A = _to(b)

sealed trait SQLJsonArray[S[+X] <: Iterable[X], A](
    val iterf: IterableFactory[S]
)(using
    ta: SQLRep[A]
)(using ev: ta.Rep <:< Column)
    extends SQLRep[S[A]]:
  final type Rep = JsonArray
  def arity: Int = 1
  def fromRow(r: List[Option[Any]]): S[A] =
    r.head.get
      .asInstanceOf[S[Any]]
      .map(any => ta.fromRow(List(Some(any))))
      .to(iterf)
  def toRow(as: S[A]): List[Option[Any]] =
    List(Some(as.map(a => ta.toRow(a).head.get)))

object SQLJsonArray:
  def apply[S[+X] <: Iterable[X], A](iterf: IterableFactory[S])(using
      ta: SQLRep[A]
  )(using ev: ta.Rep <:< Column) =
    new SQLJsonArray(iterf)(using ta) {}

trait SQLJsonADT[A] extends SQLRep[A] {
  final type Rep = JsonADT
  final def arity: Int = 1
}

object SQLJsonADT:
  inline given derived[A](using m: Mirror.Of[A]): SQLJsonADT[A] with
    def fromRow(r: List[Option[Any]]): A = r.head.get.asInstanceOf[A]
    def toRow(a: A): List[Option[Any]] = List(Some(a))

trait SQLValueClass[A] extends SQLRep[A] {
  final type Rep = ValueClass
  final def arity: Int = 1
}

object SQLValueClass:
  inline given derived[O <: Product, I, R](using m: Mirror.ProductOf[O])(using
      ev: m.MirroredElemTypes =:= (I *: EmptyTuple)
  )(using ti: SQLRep[I] { type Rep = R })(using
      ev3: R <:< Column
  ): SQLValueClass[O] with
    def fromRow(r: List[Option[Any]]): O = m.fromProduct(Tuple(ti.fromRow(r)))
    def toRow(o: O): List[Option[Any]] =
      ti.toRow(Tuple.fromProductTyped(o).productElement(0).asInstanceOf[I])

trait SQLRow[A] extends SQLRep[A] {
  final type Rep = Row
}

object SQLRow:
  def fromRowRec(row: List[Option[Any]])(reps: => List[SQLRep[Any]]): Tuple =
    reps match
      case Nil => EmptyTuple
      case h :: t =>
        val (l, r) = row.splitAt(h.asInstanceOf[SQLRep[Any]].arity)
        h.fromRow(l) *: fromRowRec(r)(t)

  def toRowRec(p: Tuple)(reps: => List[SQLRep[Any]]): List[Option[Any]] =
    (p, reps) match
      case (EmptyTuple, Nil) => Nil
      case (ph *: pt, th :: tt) =>
        th.toRow(ph) ++ toRowRec(pt)(tt)
      case _ =>
        throw GraceException(
          "Row arity and the number of values given do not match!"
        )

  inline given derived[P <: Product, I, R](using
      m: Mirror.ProductOf[P]
  ): SQLRow[P] =
    lazy val reps =
      summonAll[Tuple.Map[m.MirroredElemTypes, SQLRep]].productIterator
        .asInstanceOf[Iterator[SQLRep[Any]]]
        .toList
    new SQLRow[P]:
      def arity = reps
        .map(_.arity)
        .sum
      def fromRow(r: List[Option[Any]]): P = m.fromProduct(fromRowRec(r)(reps))
      def toRow(p: P): List[Option[Any]] = toRowRec(Tuple.fromProduct(p))(reps)
