import Schema.Weights
import fs2.{Pure, Stream}

import scala.collection.immutable.HashMap

object Schema {
  type Weights = HashMap[Vector[String], HashMap[String, Int]]

  def apply(ngrams: Stream[Pure, Vector[String]]): Schema = {
    val weights = ngrams.fold(
      new Weights()
    )((weights, ngram) => {
      val successors = weights.getOrElse(ngram.init, new HashMap[String, Int]())
      val count = successors.getOrElse(ngram.last, 0)
      val newSuccessors = successors.updated(ngram.last, count + 1)
      weights.updated(ngram.init, newSuccessors)
    }).toVector.head

    new Schema(weights)
  }
}

class Schema private(val weights: Weights) {
  def +(other: Schema): Schema = {
    val newWeights = weights.merged(other.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
      firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
        secondWord -> (thisCount + otherCount)
      }
    }

    new Schema(newWeights)
  }

  def successorsOf(tokens: Vector[String]): Option[HashMap[String, Int]] = {
    weights.get(tokens)
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
