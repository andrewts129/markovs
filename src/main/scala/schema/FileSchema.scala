package schema

import cats.effect.{ContextShift, IO}
import cats.implicits._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.implicits._

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.Random

object FileSchema {
  case class Seed(id: Long, seed: String)
  case class Ngram(id: Long, ngram: String)
  case class Successor(id: Long, successor: String, count: Int, ngramId: Long)

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def apply[S](filePath: String, dictSchema: DictSchema[S]): FileSchema[S] = {
    val transactor = Transactor.fromDriverManager[IO](
      "org.sqlite.JDBC", s"jdbc:sqlite:$filePath"
    )

    createTables(transactor).unsafeRunSync()

    new FileSchema[S](transactor)
  }

  private def createTables(transactor: Aux[IO, Unit]): IO[List[Int]] = {
    val queries = List(
      sql"DROP TABLE IF EXISTS seeds",
      sql"DROP TABLE IF EXISTS ngrams",
      sql"DROP TABLE IF EXISTS successors",
      sql"""
        CREATE TABLE seeds (
          id   INTEGER PRIMARY KEY,
          seed TEXT NOT NULL
         )
      """,
      sql"""
        CREATE TABLE ngrams (
          id    INTEGER PRIMARY KEY,
          ngram TEXT NOT NULL UNIQUE
        )
      """,
      sql"""
        CREATE TABLE successors (
          id        INTEGER PRIMARY KEY,
          ngram_id  INTEGER NOT NULL,
          successor TEXT NOT NULL,
          count     INTEGER NOT NULL,
          FOREIGN KEY(ngram_id) REFERENCES ngrams(id)
        )
      """
    )

    queries.traverse(_.update.run).transact(transactor)
  }
}

class FileSchema[S] private(transactor: Aux[IO, Unit]) extends Schema[S] {
  override def +(other: Schema[S]): Schema[S] = ???

  override def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = ???

  override def getSeed(random: Random): Option[S] = ???

  override def toDictSchema: DictSchema[S] = ???

  override def toFileSchema: FileSchema[S] = this
}
