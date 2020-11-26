import Schema.Weights
import fs2.{Stream, Pure}

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

  override def toString: String = weights.toString
}
