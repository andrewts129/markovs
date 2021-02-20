package io.andrewsmith.markovs.schema

import cats.effect.{ContextShift, IO}
import fs2.{Pure, Stream}
import io.andrewsmith.markovs.schema.DictSchema.Weights
import io.andrewsmith.markovs.schema.serialization.StringSerializable

import java.nio.file.Path
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext
import scala.util.Random


object DictSchema {
  type Weights[S] = HashMap[Vector[S], HashMap[S, Int]]

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def apply[S : StringSerializable](corpus: Stream[IO, Stream[Pure, Vector[S]]]): Stream[IO, DictSchema[S]] = {
    val numParallelStreams = Runtime.getRuntime.availableProcessors()

    corpus.balanceAvailable.take(numParallelStreams).map(
       parseCorpus(_).map {
         case (weights, seeds) => new DictSchema[S](weights, seeds)
       }
    ).parJoin(numParallelStreams).reduce(
      (first, second) => (first + second).unsafeRunSync() // DictSchema addition has no side effects, so this is safe
    )
  }

  private def parseCorpus[S : StringSerializable](corpus: Stream[IO, Stream[Pure, Vector[S]]]): Stream[IO, (Weights[S], List[S])] = {
    corpus.fold(
      (new Weights[S](), List[S]())
    )((accumulated, document) => {
      val (weights, seeds) = accumulated

      val newSeeds = document.head.toList.headOption match {
        case Some(firstNgram) => seeds :+ firstNgram.head
        case None => seeds
      }

      val newWeights = document.fold(
        weights
      )((accWeights, ngram) => {
        val successors = accWeights.getOrElse(ngram.init, new HashMap[S, Int]())
        val count = successors.getOrElse(ngram.last, 0)
        val newSuccessors = successors.updated(ngram.last, count + 1)
        accWeights.updated(ngram.init, newSuccessors)
      }).toVector.head

      (newWeights, newSeeds)
    })
  }
}

class DictSchema[S : StringSerializable](val weights: Weights[S], val seeds: Seq[S]) extends Schema[S] {
  def +(other: Schema[S]): IO[DictSchema[S]] = {
    other.toDictSchema.flatMap(otherAsDict => {
      val newWeights = weights.merged(otherAsDict.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
        firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
          secondWord -> (thisCount + otherCount)
        }
      }

      IO.pure {
        new DictSchema(newWeights, seeds ++ otherAsDict.seeds)
      }
    })
  }

  override def successorsOf(tokens: Vector[S]): IO[Option[HashMap[S, Int]]] = {
    IO.pure {
      weights.get(tokens)
    }
  }

  override def getSeed(random: Random): IO[Option[S]] = {
    IO.pure {
      seeds.size match {
        case 0 => None
        case n => Some(seeds(random.nextInt(n)))
      }
    }
  }

  override def toDictSchema: IO[DictSchema[S]] = IO.pure { this }

  override def toFileSchema(filePath: Path): IO[FileSchema[S]] = {
    FileSchema(filePath, this)
  }

  override def n: IO[Int] = {
    IO.pure {
      this.weights.keys.map(_.size).max
    }
  }

  override def toString: String = {
    val sortedWeights = weights.toVector.sortBy { case (ngram, _) => (-ngram.length, ngram.mkString) }
    sortedWeights.map { case (ngram, successors) =>
      val ngramString = ngram.mkString("(", ", ", "):")

      val successorsSorted = successors.toVector.sortBy(_._2).reverse
      val successorsStrings = successorsSorted.map { case (token, count) => s"\t$token => $count" }.toList

      (ngramString :: successorsStrings).mkString("\n")
    }.mkString("\n")
  }
}
