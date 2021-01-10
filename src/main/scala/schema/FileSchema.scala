package schema

import cats.effect.{ContextShift, IO}
import cats.implicits._
import doobie.implicits._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import schema.serialization.StringDeserializable.syntax._
import schema.serialization.StringSerializable
import schema.serialization.StringSerializable.syntax._

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.Random

// TODO get rid of all the unsafeRunSync
object FileSchema {
  case class Seed(id: Long, seed: String)
  case class Ngram(id: Long, ngram: String)
  case class Successor(id: Long, successor: String, count: Int, ngramId: Long)

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def apply[S : StringSerializable](filePath: String): FileSchema[S] = {
    val transactor = Transactor.fromDriverManager[IO](
      "org.sqlite.JDBC", s"jdbc:sqlite:$filePath"
    )

    new FileSchema[S](filePath, transactor)
  }

  def apply[S : StringSerializable](filePath: String, dictSchema: DictSchema[S]): FileSchema[S] = {
    val transactor = Transactor.fromDriverManager[IO](
      "org.sqlite.JDBC", s"jdbc:sqlite:$filePath"
    )

    createTables(transactor).unsafeRunSync()
    addSeeds(transactor, dictSchema.seeds).unsafeRunSync()
    addWeights(transactor, dictSchema.weights).unsafeRunSync()

    new FileSchema[S](filePath, transactor)
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

class FileSchema[S : StringSerializable] private(filePath: String, transactor: Aux[IO, Unit]) extends Schema[S] {
  override def +(other: Schema[S]): Schema[S] = {
    // TODO do this better
    (this.toDictSchema + other.toDictSchema).toFileSchema(this.filePath)
  }

  override def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = {
    val query =
      sql"""
        SELECT s.successor, s.count
        FROM successors s
        JOIN ngrams n ON n.id = s.ngram_id
        WHERE n.ngram = ${tokens.serialize}
      """.query[(String, Int)]

    val mappings = query.stream.map { case (successorString, count) =>
      (successorString.deserialize[S], count)
    }.fold(
      new HashMap[S, Int]()
    )((accumulatedSuccessors, row) => {
      val (successor, count) = row
      accumulatedSuccessors.updated(successor, count)
    })

    val result = mappings.transact(transactor).compile.toList.map(_.head).unsafeRunSync()
    result.size match {
      case 0 => None
      case _ => Some(result)
    }
  }

  override def getSeed(random: Random): Option[S] = {
    val query = for {
      numRows <- sql"SELECT COUNT(rowid) FROM seeds".query[Int].unique
      seed <- sql"SELECT seed FROM seeds LIMIT 1 OFFSET ${random.nextInt(numRows)}".query[String].option
    } yield seed

    query.map {
      case Some(seedString) => Some(seedString.deserialize)
      case None => None
    }.transact(transactor).unsafeRunSync()
  }

  override def toDictSchema: DictSchema[S] = {
    new DictSchema[S](
      weights.unsafeRunSync(),
      seeds.unsafeRunSync()
    )
  }

  override def toFileSchema(filePath: String): FileSchema[S] = {
    if (filePath == this.filePath) {
      this
    } else {
      FileSchema(filePath, this.toDictSchema)
    }
  }

  override def n: Int = {
    // Gets the max number of newlines in an ngram
    val query =
      sql"""
        SELECT MAX(LENGTH(ngram) - LENGTH((REPLACE(ngram, CHAR(10), ''))))
        FROM ngrams
      """.query[Int].unique

    query.transact(transactor).unsafeRunSync()
  }

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
