package graceql.context.jdbc

import scala.deriving.*
import scala.compiletime.*
import scala.collection.IterableFactory
import graceql.core.GraceException
import graceql.data.*
import graceql.typelevel.*
import scala.quoted.*
import scala.util.Try

object Row
object Base
sealed class Mapped[A, B](val from: A => B, val to: B => A)
object ValueClass
object JsonADT
object JsonArray

type Column = Base.type | Mapped[_ <: Any,_ <: Any] | ValueClass.type | JsonADT.type | JsonArray.type
type Encodings = Row.type | Column

abstract class SQLEncoding[A, E <: Encodings](val encoding: E):
  final type Encoding = E

object SQLEncoding:
  type Of[A] = SQLEncoding[A, _]
  // given jsonArraySeq[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[Seq, A] = SQLJsonArray(Seq)
  // given jsonArrayIndexedSeq[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[IndexedSeq, A] = SQLJsonArray(IndexedSeq)
  // given jsonArrayList[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[List, A] = SQLJsonArray(List)
  // given jsonArrayVector[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[Vector, A] = SQLJsonArray(Vector)
  // given jsonArrayLazyList[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[LazyList, A] = SQLJsonArray(LazyList)
  // given jsonArrayIterable[A](using ta: SQLEncoding[A])(using ev: ta.Encoding <:< Column): SQLJsonArray[Iterable, A] = SQLJsonArray(Iterable)
  given SQLBase[Int] with {}
  given SQLBase[String] with {}    

abstract class SQLBase[A] extends SQLEncoding[A, Base.type](Base)
object SQLBase

abstract class SQLMapped[A, B](encoding: Mapped[A, B])(using ta: SQLEncoding[A, _])(using ev: ta.Encoding <:< Column) extends SQLEncoding[A, Mapped[A, B]](encoding)

object SQLMapped:
  def apply[A, B](_from: A => B, _to: B => A)(using ta: SQLEncoding[A, _])(using
      ev: ta.Encoding <:< Column
  ): SQLMapped[A, B] = new SQLMapped[A, B](Mapped(_from,_to)) {}

abstract class SQLValueClass[O <: Product] extends SQLEncoding[O, ValueClass.type](ValueClass) {
  type Inner
  protected val m: Mirror.ProductOf[O] {type MirroredElemTypes = Inner *: EmptyTuple}
  protected val ev: SQLEncoding[Inner,_]#Encoding <:< Column
  final def unbox(o: O): Inner = Tuple.fromProductTyped(o)(using m).productElement(0).asInstanceOf[Inner]
  final def box(i: Inner): O = m.fromProduct(Tuple(i))
}

object SQLValueClass:
  given derived[O <: Product, I, E <: Encodings](using _m: Mirror.ProductOf[O] {type MirroredElemTypes = I *: EmptyTuple}, repi: SQLEncoding[I, _], ev0: repi.Encoding <:< Column): SQLValueClass[O] with {
    type Inner = I
    val m = _m
    val ev: SQLEncoding[Inner,_]#Encoding <:< Column = ev0.asInstanceOf[SQLEncoding[Inner,_]#Encoding <:< Column]
  }
  
abstract class SQLRow[A <: Product](using ev: Mirror.ProductOf[A]) extends SQLEncoding[A, Row.type](Row)

object SQLRow:
  given derived[P <: Product](using ev: Mirror.ProductOf[P]): SQLRow[P] with {}

// sealed trait SQLJsonArray[S[+X] <: Iterable[X], A](
//     val iterf: IterableFactory[S]
// )(using
//     val ta: SQLEncoding[A]
// )(using ev: ta.Encoding <:< Column)
//     extends SQLEncoding[S[A]]:
//   final type Encoding = JsonArray
//   final type Mirror = S[ta.Mirror]

//   final def isTuple: Boolean = false

//   def arity: Int = 1

//   final def to(a: S[A]): S[ta.Mirror] = a.map(ta.to).to(iterf)

//   final def from(m: S[ta.Mirror]): S[A] = m.map(ta.from).to(iterf)  

//   protected def listToMirror(r: List[Option[Any]]): S[ta.Mirror] =
//     r.head.get
//       .asInstanceOf[S[Any]]
//       .map(any => ta.listToMirror(List(Some(any))))
//       .to(iterf)
//   def mirrorToList(as: S[ta.Mirror]): List[Option[Any]] =
//     List(Some(as.map(a => ta.mirrorToList(a).head.get)))

// object SQLJsonArray:
//   def apply[S[+X] <: Iterable[X], A](iterf: IterableFactory[S])(using
//       ta: SQLEncoding[A]
//   )(using ev: ta.Encoding <:< Column) =
//     new SQLJsonArray(iterf)(using ta) {}

// trait SQLJsonADT[A] extends SQLEncoding[A] {
//   final type Encoding = JsonADT
//   final type Mirror = Json
//   final def isTuple: Boolean = false
//   final def arity: Int = 1
// }

// object SQLJsonADT:
//   inline given derived[A](using m: Mirror.Of[A]): SQLJsonADT[A] with
//     final def to(a: A): Json = ???
//     final def from(m: Json): A = ???  
//     protected def listToMirror(r: List[Option[Any]]): Json = r.head.get.asInstanceOf[Json]
//     def mirrorToList(a: Json): List[Option[Any]] = List(Some(a))

trait SQLMirror[A, M]:
  self =>
  final type Mirror = M  
  def isTuple: Boolean
  def arity: Int
  def to(a: A): M
  def from(m: M): A
  protected def listToMirror(r: List[Option[Any]]): M
  def mirrorToList(a: M): List[Option[Any]]
  object dynamic:
    inline def from(r: List[Option[Any]]): Try[A] = Try{self.from(listToMirror(r))}
    inline def to(a: A): List[Option[Any]] = mirrorToList(self.to(a))
    def unapply(r: List[Option[Any]]) : Option[(A, M)] = 
      Try { listToMirror(r) }.toOption.map(m => (self.from(m),m))

object SQLMirror:
  type Of[A] = SQLMirror[A, _]

  protected type MapTo[X,F[_]] = X match
    case Tuple => Tuple.Map[X, F]
    case _ => F[X]

  protected type ToTuple[X] <: Tuple = X match
    case Tuple => Tuple.Map[X, [x] =>> x]
    case _ => X *: EmptyTuple           

  given option[A, E <: Encodings, M](using ta: SQLMirror[A, M]): SQLMirror[Option[A], MapTo[M, Option]] with
    def arity: Int = ta.arity
    def isTuple = ta.isTuple
    def to(a: Option[A]): Mirror = 
      if isTuple then
        a.fold(Tuple.fromArray(Array.fill(arity)(None))){i => 
          val elems = ta.to(i).asInstanceOf[Tuple].productIterator.toArray.map(Some(_))
          Tuple.fromArray(elems)  
        }.asInstanceOf[Mirror]
      else 
        a.fold(None)(i => Some(ta.to(i))).asInstanceOf[Mirror]  
    def from(m: Mirror): Option[A] =
      if isTuple then
        val vs = m.asInstanceOf[Tuple].productIterator.toArray.map(_.asInstanceOf[Option[Any]])
        if vs.forall(_.isDefined) then
          Some(ta.from(Tuple.fromArray(vs.map(_.get)).asInstanceOf[M]))    
        else 
          None
      else
        m.asInstanceOf[Option[M]].map(ta.from)      

    protected def listToMirror(r: List[Option[Any]]): Mirror =
      if isTuple then
        if r.forall(_.isEmpty) then
          Tuple.fromArray(r.toArray).asInstanceOf[Mirror]
        else
          val elems = ta.listToMirror(r).asInstanceOf[Tuple].productIterator.toArray
          Tuple.fromArray(elems).asInstanceOf[Mirror]
      else
        r.head.fold(None)(i => Some(ta.listToMirror(List(Some(i))))).asInstanceOf[Mirror]    
          
    def mirrorToList(a: Mirror): List[Option[Any]] = 
      if isTuple then
        val elems = a.asInstanceOf[Tuple].productIterator.toArray.map(_.asInstanceOf[Option[Any]])
        if (elems.forall(_.isDefined)) then
          ta.mirrorToList(Tuple.fromArray(elems.map(_.get)).asInstanceOf[M]).map(Some(_))
        else
          List.fill(arity)(None)    
      else
        a.asInstanceOf[Option[M]].fold(Nil)(i => List(Some(ta.mirrorToList(i))))  

  given emptyTuple: SQLMirror[EmptyTuple, EmptyTuple] with
    def isTuple = true
    def arity: Int = 0
    def to(a: EmptyTuple): EmptyTuple = a
    def from(m: EmptyTuple): EmptyTuple = m 
    protected def listToMirror(r: List[Option[Any]]): EmptyTuple =
      assert(r.isEmpty)
      EmptyTuple
    def mirrorToList(a: EmptyTuple): List[Option[Any]] = Nil

  given inductiveTuple[H, T <: Tuple, MH, MT <: Tuple](using th: SQLMirror[H, MH])(using tt: SQLMirror[T, MT]): SQLMirror[H *: T, Tuple.Concat[ToTuple[MH], MT]] with
    def isTuple = true
    def arity: Int = th.arity + tt.arity
    def to(a: H *: T): Mirror= 
      val mh = th.to(a.head)
      val mhTuple = if th.isTuple then mh.asInstanceOf[Tuple] else Tuple(mh)
      (mhTuple ++ tt.to(a.tail)).asInstanceOf[Mirror]
    def from(m: Mirror): H *: T = 
      val (l, r) = m.productIterator.toArray.splitAt(th.arity)
      val formatted = (if th.isTuple then Tuple.fromArray(l) else l.head).asInstanceOf[MH]
      th.from(formatted) *: tt.from(Tuple.fromArray(r).asInstanceOf[MT])

    protected def listToMirror(row: List[Option[Any]]): Mirror =
      val (l, r) = row.splitAt(th.arity)
      val tup = th.listToMirror(l) match 
        case x: Tuple => x
        case x => Tuple(x)
      (tup ++ tt.listToMirror(r)).asInstanceOf[Mirror]

    def mirrorToList(a: Mirror): List[Option[Any]] =
      val (l, r) = a.productIterator.toArray.splitAt(th.arity)
      val formatted = (if th.isTuple then Tuple.fromArray(l) else l.head).asInstanceOf[MH]
      th.mirrorToList(formatted) ++ tt.mirrorToList(Tuple.fromArray(r).asInstanceOf[MT])

  given base[A](using SQLBase[A]): SQLMirror[A, A] with
    final def isTuple: Boolean = false
    def arity = 1
    final def to(a: A): A = a
    
    final def from(m: A): A = m

    final protected def listToMirror(r: List[Option[Any]]): A =
      r.head.get.asInstanceOf[A]

    final def mirrorToList(m: A): List[Option[Any]] = List(Some(m))

  given product[P <: Product, M <: Tuple](using ev: SQLRow[P] ,prodMirr: Mirror.ProductOf[P], sqlTup: SQLMirror[prodMirr.MirroredElemTypes, M]): SQLMirror[P, M] with {
        def arity: Int = sqlTup.arity

        def isTuple = true
        def to(a: P): M = sqlTup.to(Tuple.fromProductTyped(a))
        def from(m: M): P = 
          prodMirr.fromProduct(sqlTup.from(m)) 
        protected def listToMirror(r: List[Option[Any]]): M = sqlTup.listToMirror(r)
        def mirrorToList(a: M): List[Option[Any]] = sqlTup.mirrorToList(a)
  }

  given mapped[A, B, M](using mppd: SQLMapped[A,B], ta: SQLMirror[A, M]): SQLMirror[B, M] with

    final def isTuple: Boolean = false

    def arity = 1

    final def to(a: B): M = ta.to(mppd.encoding.to(a))

    final def from(m: M): B = mppd.encoding.from(ta.from(m))

    final protected def listToMirror(r: List[Option[Any]]): M = ta.listToMirror(r)
    
    final def mirrorToList(m: M): List[Option[Any]] = ta.mirrorToList(m)

  given valueClass[O <: Product, I, M](using vc: SQLValueClass[O], ti: SQLMirror[vc.Inner, M]): SQLMirror[O, M] with
    final def to(o: O): M = ti.to(vc.unbox(o))
    final def from(m: M): O = vc.box(ti.from(m))

    def isTuple = false
    def arity = 1
    protected def listToMirror(r: List[Option[Any]]): M = ti.listToMirror(r)
    def mirrorToList(m: M): List[Option[Any]] = ti.mirrorToList(m)
