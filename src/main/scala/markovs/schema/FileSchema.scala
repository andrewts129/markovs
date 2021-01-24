package markovs.schema

import cats.effect.{ContextShift, IO}
import cats.implicits._
import doobie.FC
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.{Pure, Stream}
import markovs.schema.serialization.StringDeserializable.syntax._
import markovs.schema.serialization.StringSerializable
import markovs.schema.serialization.StringSerializable.syntax._

import java.nio.file.{Files, Path}
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.Random

object FileSchema {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def apply[S : StringSerializable](filePath: Path): FileSchema[S] = {
    new FileSchema[S](filePath, getTransactor(filePath))
  }

  def apply[S : StringSerializable](filePath: Path, corpus: Stream[IO, Stream[Pure, Vector[S]]]): Stream[IO, FileSchema[S]] = {
    val dictSchemas = corpus.chunkN(1000).map(Stream.chunk).flatMap(DictSchema(_))
    fromDictSchemas(filePath, dictSchemas).evalMap(_.compact)
  }

  def apply[S : StringSerializable](filePath: Path, dictSchema: DictSchema[S]): IO[FileSchema[S]] = {
    val transactor = getTransactor(filePath)

    for {
      _ <- createTables(transactor)
      _ <- addSeeds(transactor, dictSchema.seeds)
      _ <- insertWeights(transactor, dictSchema.weights)
      schema <- IO { new FileSchema[S](filePath, transactor) }
      compactedSchema <- schema.compact
    } yield compactedSchema
  }

  private def fromDictSchemas[S : StringSerializable](filePath: Path, dictSchemas: Stream[IO, DictSchema[S]]): Stream[IO, FileSchema[S]] = {
    val headAsFileSchema = dictSchemas.head.evalMap(_.toFileSchema(filePath))

    // Honestly I'm shocked this works
    headAsFileSchema.evalMap(
      fileSchema => dictSchemas.drop(1).evalMap(fileSchema + _).compile.toList.map(_.lastOption match {
        case Some(fileSchemaWithAdditions) => fileSchemaWithAdditions
        case None => fileSchema // When the chunkN size is greater than the corpus size, dictSchemas.drop(1) is empty, so we need to get a return value elsewhere
      })
    )
  }

  private def getTransactor(filePath: Path): Transactor[IO] = {
    Transactor.fromDriverManager[IO](
      "org.sqlite.JDBC", s"jdbc:sqlite:${filePath.toAbsolutePath.toString}"
    )
  }

  private def createTables(transactor: Transactor[IO]): IO[List[Int]] = {
    val queries = List(
      sql"DROP TABLE IF EXISTS seeds",
      sql"DROP TABLE IF EXISTS successors",
      sql"""
        CREATE TABLE seeds (
          id   INTEGER PRIMARY KEY,
          seed TEXT NOT NULL
         )
      """,
      sql"""
        CREATE TABLE successors (
          id        INTEGER PRIMARY KEY,
          ngram     TEXT NOT NULL,
          successor TEXT NOT NULL,
          count     INTEGER NOT NULL,
          UNIQUE(ngram, successor)
        )
      """,
      sql"CREATE INDEX ngram_index ON successors (ngram)"
    )

    queries.traverse(_.update.run).transact(transactor)
  }

  private def addSeeds[S : StringSerializable](transactor: Transactor[IO], seeds: Seq[S]): IO[List[Int]] = {
    val queries = seeds.map(seed =>
      sql"INSERT INTO seeds (seed) VALUES (${seed.serialize})".update.run
    )

    queries.toList.sequence.transact(transactor)
  }

  private def insertWeights[S : StringSerializable](transactor: Transactor[IO], weights: HashMap[Vector[S], HashMap[S, Int]]): IO[List[Int]] = {
    val queries = weights.flatMap {
      case (ngram, successors) => successors.map {
        case (successor, count) =>
          sql"INSERT INTO successors (ngram, successor, count) VALUES (${ngram.serialize}, ${successor.serialize}, $count)".update.run
      }
    }

    queries.toList.sequence.transact(transactor)
  }

  private def updateWeights[S : StringSerializable](transactor: Transactor[IO], weights: HashMap[Vector[S], HashMap[S, Int]]): IO[List[Int]] = {
    val queries = weights.flatMap {
      case (ngram, successors) => successors.map {
        case (successor, count) =>
          sql"""
            INSERT INTO successors (ngram, successor, count) VALUES (${ngram.serialize}, ${successor.serialize}, $count)
            ON CONFLICT(ngram, successor) DO UPDATE SET count = count + $count;
          """.update.run
      }
    }

    queries.toList.sequence.transact(transactor)
  }

  private def vacuumTables(transactor: Transactor[IO]): IO[Int] = {
    (FC.setAutoCommit(true) *> sql"VACUUM".update.run <* FC.setAutoCommit(false)).transact(transactor)
  }
}

class FileSchema[S : StringSerializable] private(filePath: Path, transactor: Transactor[IO]) extends Schema[S] {
  override def +(other: Schema[S]): IO[FileSchema[S]] = {
    other.toDictSchema.flatMap {
      otherAsDict => {
        val seedUpdate = FileSchema.addSeeds(transactor, otherAsDict.seeds)
        val weightUpdate = FileSchema.updateWeights(transactor, otherAsDict.weights)
        (seedUpdate, weightUpdate).mapN((_, _) => this)
      }
    }
  }

  override def successorsOf(tokens: Vector[S]): IO[Option[HashMap[S, Int]]] = {
    val query =
      sql"""
        SELECT successor, count
        FROM successors
        WHERE ngram = ${tokens.serialize}
      """.query[(String, Int)]

    val mappings = query.stream.map { case (successorString, count) =>
      (successorString.deserialize[S], count)
    }.fold(
      new HashMap[S, Int]()
    )((accumulatedSuccessors, row) => {
      val (successor, count) = row
      accumulatedSuccessors.updated(successor, count)
    })

    mappings.transact(transactor).compile.toList.map(_.head).map(
      result => result.size match {
        case 0 => None
        case _ => Some(result)
      }
    )
  }

  override def getSeed(random: Random): IO[Option[S]] = {
    val query = for {
      numRows <- sql"SELECT COUNT(rowid) FROM seeds".query[Int].unique
      seed <- sql"SELECT seed FROM seeds LIMIT 1 OFFSET ${random.nextInt(numRows)}".query[String].option
    } yield seed

    query.map {
      case Some(seedString) => Some(seedString.deserialize)
      case None => None
    }.transact(transactor)
  }

  override def toDictSchema: IO[DictSchema[S]] = {
    for {
      weights <- this.weights
      seeds <- this.seeds
      schema <- IO { new DictSchema[S](weights, seeds) }
    } yield schema
  }

  override def toFileSchema(filePath: Path): IO[FileSchema[S]] = {
    if (Files.isSameFile(filePath, this.filePath)) {
      IO.pure { this }
    } else {
      this.toDictSchema.flatMap(
        asDict => FileSchema[S](filePath, asDict)
      )
    }
  }

  override def n: IO[Int] = {
    // Gets the max number of newlines in an ngram
    val query =
      sql"""
        SELECT MAX(LENGTH(ngram) - LENGTH((REPLACE(ngram, CHAR(10), ''))))
        FROM successors
      """.query[Int].unique

    query.transact(transactor)
  }

  private def seeds: IO[Seq[S]] = {
    val query = sql"SELECT seed FROM seeds".query[String]
    query.stream.map(_.deserialize[S]).transact(transactor).compile.toVector
  }

  private def weights: IO[HashMap[Vector[S], HashMap[S, Int]]] = {
    val query =
      sql"""
        SELECT ngram, successor, count
        FROM successors
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

  private def compact: IO[FileSchema[S]] = {
    FileSchema.vacuumTables(transactor).map(_ => this)
  }
}
