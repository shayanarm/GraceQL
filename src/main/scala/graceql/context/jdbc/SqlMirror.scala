package graceql.context.jdbc

import scala.deriving.*
import scala.compiletime.*
import scala.collection.IterableFactory
import graceql.core.GraceException
import graceql.data.*
import graceql.typelevel.*
import scala.quoted.*
import scala.util.Try

sealed class Row
sealed class Base
sealed class Mapped
sealed class ValueClass
// sealed class JsonADT
// sealed class JsonArray

type Column = Base | Mapped | ValueClass // | JsonADT | JsonArray
type Encodings = Row | Column

abstract class SqlEncoding[A, E <: Encodings]:
  final type Encoding = E

object SqlEncoding:
  type Of[A] = SqlEncoding[A, _]
  // given jsonArraySeq[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[Seq, A] = SqlJsonArray(Seq)
  // given jsonArrayIndexedSeq[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[IndexedSeq, A] = SqlJsonArray(IndexedSeq)
  // given jsonArrayList[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[List, A] = SqlJsonArray(List)
  // given jsonArrayVector[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[Vector, A] = SqlJsonArray(Vector)
  // given jsonArrayLazyList[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[LazyList, A] = SqlJsonArray(LazyList)
  // given jsonArrayIterable[A](using ta: SqlEncoding[A])(using ev: ta.Encoding <:< Column): SqlJsonArray[Iterable, A] = SqlJsonArray(Iterable)
  given SqlBase[String] with {}
  given SqlBase[Char] with {}
  given SqlBase[Byte] with {}
  given SqlBase[Boolean] with {}
  given SqlBase[Short] with {}      
  given SqlBase[Int] with {}    
  given SqlBase[Long] with {}    
  given SqlBase[Float] with {}    
  given SqlBase[Double] with {}
  given SqlBase[BigDecimal] with {}
  given SqlBase[BigInt] with {}
  given SqlBase[Unit] with {}    
  given SqlBase[java.sql.Date] with {}
      
  given option[A, E <: Encodings](using ta: SqlEncoding[A, E]): SqlEncoding[Option[A], E] with {}
  given tuples[T](using ev: T <:< Tuple): SqlEncoding[T, Row] with {}

abstract class SqlBase[A] extends SqlEncoding[A, Base]
object SqlBase

trait SqlMapped[A, B](using ta: SqlEncoding[A, _], ev: ta.Encoding <:< Column) extends SqlEncoding[A, Mapped]:
  final type From = A
  def from(a: A): B
  def to(b: B): A

object SqlMapped:
  type To[B] = SqlMapped[_, B]

abstract class SqlValueClass[O <: Product] extends SqlEncoding[O, ValueClass] {
  type Inner
  protected val m: Mirror.ProductOf[O] {type MirroredElemTypes = Inner *: EmptyTuple}
  protected val ev: SqlEncoding[Inner,_]#Encoding <:< Column
  final def unbox(o: O): Inner = Tuple.fromProductTyped(o)(using m).productElement(0).asInstanceOf[Inner]
  final def box(i: Inner): O = m.fromProduct(Tuple(i))
}

object SqlValueClass:
  given derived[O <: Product, I, E <: Encodings](using _m: Mirror.ProductOf[O] {type MirroredElemTypes = I *: EmptyTuple}, repi: SqlEncoding[I, _], ev0: repi.Encoding <:< Column): SqlValueClass[O] with {
    type Inner = I
    val m = _m
    val ev: SqlEncoding[Inner,_]#Encoding <:< Column = ev0.asInstanceOf[SqlEncoding[Inner,_]#Encoding <:< Column]
  }
  
abstract class SqlRow[A <: Product](using ev: Mirror.ProductOf[A]) extends SqlEncoding[A, Row]

object SqlRow:
  given derived[P <: Product](using ev: Mirror.ProductOf[P]): SqlRow[P] with {}

// sealed trait SqlJsonArray[S[+X] <: Iterable[X], A](
//     val iterf: IterableFactory[S]
// )(using
//     val ta: SqlEncoding[A]
// )(using ev: ta.Encoding <:< Column)
//     extends SqlEncoding[S[A]]:
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

// object SqlJsonArray:
//   def apply[S[+X] <: Iterable[X], A](iterf: IterableFactory[S])(using
//       ta: SqlEncoding[A]
//   )(using ev: ta.Encoding <:< Column) =
//     new SqlJsonArray(iterf)(using ta) {}

// trait SqlJsonADT[A] extends SqlEncoding[A] {
//   final type Encoding = JsonADT
//   final type Mirror = Json
//   final def isTuple: Boolean = false
//   final def arity: Int = 1
// }

// object SqlJsonADT:
//   inline given derived[A](using m: Mirror.Of[A]): SqlJsonADT[A] with
//     final def to(a: A): Json = ???
//     final def from(m: Json): A = ???  
//     protected def listToMirror(r: List[Option[Any]]): Json = r.head.get.asInstanceOf[Json]
//     def mirrorToList(a: Json): List[Option[Any]] = List(Some(a))

trait SqlMirror[A, M]:
  self =>
  final type Mirror = M  
  def isTuple: Boolean
  def arity: Int
  def to(a: A): M
  def from(m: M): A
  protected def listToMirror(r: List[Option[Any]]): M
  def mirrorToList(a: M): List[Option[Any]]
  object dynamic:
    inline def to(a: A): List[Option[Any]] = mirrorToList(self.to(a))    
    inline def from(r: List[Option[Any]]): Try[A] = Try{self.from(listToMirror(r))}
    def unapply(r: List[Option[Any]]) : Option[(A, M)] = 
      Try { listToMirror(r) }.toOption.map(m => (self.from(m),m))

object SqlMirror:
  type Of[A] = SqlMirror[A, _]

  protected type MapTo[X,F[_]] = X match
    case Tuple => Tuple.Map[X, F]
    case _ => F[X]

  protected type ToTuple[X] <: Tuple = X match
    case Tuple => Tuple.Map[X, [x] =>> x]
    case _ => X *: EmptyTuple           

  given option[A, E <: Encodings, M](using ta: SqlMirror[A, M]): SqlMirror[Option[A], MapTo[M, Option]] with
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

  given emptyTuple: SqlMirror[EmptyTuple, EmptyTuple] with
    def isTuple = true
    def arity: Int = 0
    def to(a: EmptyTuple): EmptyTuple = a
    def from(m: EmptyTuple): EmptyTuple = m 
    protected def listToMirror(r: List[Option[Any]]): EmptyTuple =
      assert(r.isEmpty)
      EmptyTuple
    def mirrorToList(a: EmptyTuple): List[Option[Any]] = Nil

  given inductiveTuple[H, T <: Tuple, MH, MT <: Tuple](using th: SqlMirror[H, MH])(using tt: SqlMirror[T, MT]): SqlMirror[H *: T, Tuple.Concat[ToTuple[MH], MT]] with
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

  given base[A](using SqlBase[A]): SqlMirror[A, A] with
    final def isTuple: Boolean = false
    def arity = 1
    final def to(a: A): A = a
    
    final def from(m: A): A = m

    final protected def listToMirror(r: List[Option[Any]]): A =
      r.head.get.asInstanceOf[A]

    final def mirrorToList(m: A): List[Option[Any]] = List(Some(m))

  given product[P <: Product, M <: Tuple](using ev: SqlRow[P] ,prodMirr: Mirror.ProductOf[P], sqlTup: SqlMirror[prodMirr.MirroredElemTypes, M]): SqlMirror[P, M] with {
        def arity: Int = sqlTup.arity

        def isTuple = true
        def to(a: P): M = sqlTup.to(Tuple.fromProductTyped(a))
        def from(m: M): P = 
          prodMirr.fromProduct(sqlTup.from(m)) 
        protected def listToMirror(r: List[Option[Any]]): M = sqlTup.listToMirror(r)
        def mirrorToList(a: M): List[Option[Any]] = sqlTup.mirrorToList(a)
  }

  given mapped[A, B, M](using mppd: SqlMapped[A, B], ta: SqlMirror[A, M]): SqlMirror[B, M] with

    final def isTuple: Boolean = false

    def arity = 1

    final def to(a: B): M = ta.to(mppd.to(a))

    final def from(m: M): B = mppd.from(ta.from(m))

    final protected def listToMirror(r: List[Option[Any]]): M = ta.listToMirror(r)
    
    final def mirrorToList(m: M): List[Option[Any]] = ta.mirrorToList(m)

  given valueClass[O <: Product, M](using vc: SqlValueClass[O], ti: SqlMirror[vc.Inner, M]): SqlMirror[O, M] with
    final def to(o: O): M = ti.to(vc.unbox(o))
    final def from(m: M): O = vc.box(ti.from(m))

    def isTuple = false
    def arity = 1
    protected def listToMirror(r: List[Option[Any]]): M = ti.listToMirror(r)
    def mirrorToList(m: M): List[Option[Any]] = ti.mirrorToList(m)
