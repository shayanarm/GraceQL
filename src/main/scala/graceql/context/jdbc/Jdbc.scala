package graceql.context.jdbc

import graceql.core.*
import java.sql.{Connection => JConnection, ResultSet}

final case class Table[V, +A]()

object Table:
  given execSync[V, A, L, T]
      : Execute[[x] =>> Table[V, x], DBIO, JConnection, A, A] with
    def apply(compiled: DBIO[A], conn: JConnection): A =
      try
        compiled match
          case DBIO.Query(q, p) =>
            val stmt = conn.createStatement()
            p(stmt.executeQuery(q))
          case DBIO.Update(s) =>
            val stmt = conn.createStatement()            
            stmt.executeUpdate(s)
          case DBIO.Statement(s) =>
            val stmt = conn.createStatement()            
            stmt.execute(s)
            ()
          case DBIO.Pure(run) => run() 
      catch case e => throw GraceException(e.getMessage, e)

enum DBIO[+A]:
  case Query[+A](val query: String, val parse: ResultSet => A) extends DBIO[A]
  case Update(val stmt: String) extends DBIO[Int]
  case Statement(val stmt: String) extends DBIO[Unit]
  case Pure[A](val run: () => A) extends DBIO[A]
  def executableStmt: Option[String] = this match
    case Query(q,_) => Some(q)
    case Update(s) => Some(s)
    case Statement(s) => Some(s)
    case Pure(_) => None
    
  override def toString() = this match
    case Query(q,_) => s"Serialized($q)"
    case Update(s) => s"Serialized($s)"
    case Statement(s) => s"Serialized($s)"
    case Pure(run) => run.toString() // Not to be evaluated

object DBIO:
  def query[A](query: String, parse: ResultSet => A): DBIO[A] =
    DBIO.Query(query, parse)
  def update(stmt: String): DBIO[Int] = DBIO.Update(stmt)
  def statement(stmt: String): DBIO[Unit] = DBIO.Statement(stmt)
  inline def literal[A](value: A): DBIO[A] = pure(() => value)
  def pure[A](run: () => A): DBIO[A] = DBIO.Pure(run)

trait JdbcQueryContext[V, S[+X] <: Iterable[X]]
    extends QueryContext[[x] =>> Table[V, x], S]:

  final type Native[+A] = DBIO[A]

  final type Connection = JConnection      