package graceql.core

import scala.quoted.*
import graceql.data.*
import graceql.annotation.terminal
import scala.annotation.targetName
import scala.reflect.TypeTest

trait SqlLike[R[_], M[_]] extends Queryable[[x] =>> Source[R, M, x]]:
  
  type WriteResult

  final type Read[T] = T match
    case (k, grpd) => (k, Read[grpd])
    case Source[R, M, a] => M[Read[a]]
    case _ => T
    
  extension [A](a: A)
    @terminal
    def read: Read[A]
  extension [A](values: M[A])
    @targetName("valuesAsSource")
    inline def asSource: Source[R, M, A] = Source.Values(values)
  extension [A](ref: R[A])
    @targetName("refAsSource")
    inline def asSource: Source[R, M, A] = Source.Ref(ref)
    @terminal
    def insertMany(a: Source[R, M, A]): WriteResult
    @terminal
    inline def ++=(a: Source[R, M, A]): WriteResult = insertMany(a)   
    @terminal
    inline def insert(a: A): WriteResult = insertMany(a.pure)
    @terminal
    inline def +=(a: A): WriteResult = insert(a)
    @terminal
    def update(predicate: A => Boolean)(a: A => A): WriteResult
    @terminal
    def delete(a: A => Boolean): WriteResult
    @terminal
    inline def truncate(): WriteResult = delete(_ => true)
      

trait Context[R[_], M[_]]:
  type Compiled[A]
  class Exe[A](val compiled: Compiled[A])
  inline def apply[A](inline query: SqlLike[R, M] ?=> A): Exe[A] =
    Exe(compile(query))
  inline def compile[A](inline query: SqlLike[R, M] ?=> A): Compiled[A]    
