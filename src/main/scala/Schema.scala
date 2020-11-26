import Schema.Weights
import fs2.{Stream, Pure}

import scala.collection.immutable.HashMap

object Schema {
  type Weights = HashMap[String, HashMap[String, Int]]

  def apply(ngrams: Stream[Pure, (String, String)]): Schema = {
    val weights = ngrams.fold(
      new Weights()
    )((weights, ngram) => {
      val successors = weights.getOrElse(ngram._1, new HashMap[String, Int]())
      val count = successors.getOrElse(ngram._2, 0)
      val newSuccessors = successors.updated(ngram._2, count + 1)
      weights.updated(ngram._1, newSuccessors)
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

  def successorsOf(token: String): Option[HashMap[String, Int]] = {
    weights.get(token)
  }

  override def toString: String = weights.toString
}
