import Schema.Weights
import fs2.{Pure, Stream}

import scala.collection.immutable.HashMap
import scala.util.Random


object Schema {
  type Weights[S] = HashMap[Vector[S], HashMap[S, Int]]

  def apply[S](ngrams: Stream[Pure, Vector[S]]): Schema[S] = {
    val weights = ngrams.fold(
      new Weights[S]()
    )((weights, ngram) => {
      val successors = weights.getOrElse(ngram.init, new HashMap[S, Int]())
      val count = successors.getOrElse(ngram.last, 0)
      val newSuccessors = successors.updated(ngram.last, count + 1)
      weights.updated(ngram.init, newSuccessors)
    }).toVector.head

    val seeds = weights.size match {
      case 0 => Vector()
      case _ => Vector(ngrams.head.toVector.head.head)
    }

    new Schema(weights, seeds)
  }
}

class Schema[S] private(val weights: Weights[S], val seeds: Seq[S]) {
  def +(other: Schema[S]): Schema[S] = {
    val newWeights = weights.merged(other.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
      firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
        secondWord -> (thisCount + otherCount)
      }
    }

    new Schema(newWeights, seeds ++ other.seeds)
  }

  def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = {
    weights.get(tokens)
  }

  def getSeed(random: Random): Option[S] = {
    seeds.size match {
      case 0 => None
      case n => Some(seeds(random.nextInt(n)))
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
