package graceql.core

import scala.quoted.*
import graceql.data.*
import graceql.annotation.terminal
import scala.annotation.targetName
import scala.reflect.TypeTest

trait SqlLike[R[_], M[_]] extends Queryable[[x] =>> Source[R, M, x]]:
  type Read[T] = T match
    case Source[R,M,a] => M[a]
    case _ => T
    
  extension [A](a: A)(using tt: TypeTest[A,Read[A]])
    @terminal
    def read: Read[A]
  extension [A](values: M[A])
    @targetName("valuesAsSource")
    inline def asSource: Source[R, M, A] = Source.Values(values)
  extension [A](ref: R[A])
    @targetName("refAsSource")
    inline def asSource: Source[R, M, A] = Source.Ref(ref)
    @terminal
    def insertMany(a: Source[R, M, A]): Int
    @terminal
    inline def ++=(a: Source[R, M, A]): Int = insertMany(a)   
    @terminal
    inline def insert(a: A): Int = insertMany(a.pure)
    @terminal
    inline def +=(a: A): Int = insert(a)
    @terminal
    def update(predicate: A => Boolean)(a: A => A): Int
    @terminal
    def delete(a: A => Boolean): Int
    @terminal
    inline def truncate(): Int = delete(_ => true)
  
trait Context[R[_], M[_]]:
  inline def apply[A](inline query: SqlLike[R, M] ?=> A): A    