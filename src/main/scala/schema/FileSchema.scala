package schema

import cats.effect.{ContextShift, IO}
import cats.implicits._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.implicits._
import schema.StringDeserializable.syntax._
import schema.StringSerializable.syntax._

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.Random

object FileSchema {
  case class Seed(id: Long, seed: String)
  case class Ngram(id: Long, ngram: String)
  case class Successor(id: Long, successor: String, count: Int, ngramId: Long)

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def apply[S : StringSerializable](filePath: String, dictSchema: DictSchema[S]): FileSchema[S] = {
    val transactor = Transactor.fromDriverManager[IO](
      "org.sqlite.JDBC", s"jdbc:sqlite:$filePath"
    )

    // TODO can I do this without unsafeRunSync?
    createTables(transactor).unsafeRunSync()
    addSeeds(transactor, dictSchema.seeds).unsafeRunSync()
    addWeights(transactor, dictSchema.weights).unsafeRunSync()

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

  private def addSeeds[S : StringSerializable](transactor: Aux[IO, Unit], seeds: Seq[S]): IO[List[Int]] = {
    val inserts = seeds.map(seed =>
      sql"INSERT INTO seeds (seed) VALUES (${seed.serialize})".update.run
    )

    inserts.toList.sequence.transact(transactor)
  }

  private def addWeights[S : StringSerializable](transactor: Aux[IO, Unit], weights: HashMap[Vector[S], HashMap[S, Int]]): IO[List[Long]] = {
    val inserts = weights.map { case (ngram, successors) =>
      for {
        _ <- sql"INSERT INTO ngrams (ngram) VALUES (${ngram.serialize})".update.run
        id <- sql"SELECT last_insert_rowid()".query[Long].unique
        _ <- successorInserts(id, successors)
      } yield id
    }

    inserts.toList.sequence.transact(transactor)
  }

  private def successorInserts[S : StringSerializable](ngramId: Long, successors: HashMap[S, Int]): doobie.ConnectionIO[List[Int]] = {
    successors.map { case (successor, count) =>
      sql"INSERT INTO successors (ngram_id, successor, count) VALUES ($ngramId, ${successor.serialize}, $count)".update.run
    }.toList.sequence
  }
}

class FileSchema[S : StringSerializable] private(transactor: Aux[IO, Unit]) extends Schema[S] {
  override def +(other: Schema[S]): Schema[S] = {
    // TODO do this better
    (this.toDictSchema + other.toDictSchema).toFileSchema
  }

  override def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = ???

  override def getSeed(random: Random): Option[S] = ???

  override def toDictSchema: DictSchema[S] = {
    // TODO can I do this without unsafeRunSync?
    new DictSchema[S](
      weights.unsafeRunSync(),
      seeds.unsafeRunSync()
    )
  }

  override def toFileSchema: FileSchema[S] = this

  private def seeds: IO[Seq[S]] = {
    val query = sql"SELECT seed FROM seeds".query[String]
    query.stream.map(_.deserialize[S]).transact(transactor).compile.toVector
  }

  private def weights: IO[HashMap[Vector[S], HashMap[S, Int]]] = {
    val query =
      sql"""
        SELECT n.ngram, s.successor, s.count
        FROM ngrams n
        JOIN successors s ON n.id = s.ngram_id
      """.query[(String, String, Int)]

    val mappings = query.stream.map { case (ngramString, successorString, count) =>
      val ngram = ngramString.deserialize[Vector[S]]
      val successor = successorString.deserialize[S]
      (ngram, successor, count)
    }.fold(
      new HashMap[Vector[S], HashMap[S, Int]]()
    )((accumulatedWeights, row) => {
      val (ngram, successorWord, count) = row
      val currentSuccessors = accumulatedWeights.getOrElse(ngram, new HashMap[S, Int]())
      val updatedSuccessors = currentSuccessors.updated(successorWord, count)
      accumulatedWeights.updated(ngram, updatedSuccessors)
    })

    mappings.transact(transactor).compile.toList.map(_.head)
  }
}
