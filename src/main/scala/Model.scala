import Model.Weights
import fs2.{Pure, Stream}

import scala.collection.immutable.HashMap

object Model {
  type Weights = HashMap[String, HashMap[String, Int]]

  def apply[F[_]](corpus: Stream[F, String]): Stream[F, Model] = {
    corpus.map(Model(_)).reduce(_ + _)
  }

  def apply(document: String): Model = {
    val ngrams = asNGrams(tokenize(document))
    val weights = ngrams.fold(
      new Weights()
    )((weights, ngram) => {
      val successors = weights.getOrElse(ngram._1, new HashMap[String, Int]())
      val count = successors.getOrElse(ngram._2, 0)
      val newSuccessors = successors.updated(ngram._2, count + 1)
      weights.updated(ngram._1, newSuccessors)
    }).toVector.head

    new Model(weights)
  }

  private def asNGrams(tokens: Stream[Pure, String]): Stream[Pure, (String, String)] = {
    tokens.zipWithNext.collect { case (first, Some(second)) => (first, second) }
  }

  private def tokenize(string: String): Stream[Pure, String] = Stream.emits(string.split("\\s+"))
}

class Model private(val weights: Weights) {
  def +(other: Model): Model = {
    val newWeights = weights.merged(other.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
      firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
        secondWord -> (thisCount + otherCount)
      }
    }

    new Model(newWeights)
  }

  override def toString: String = weights.toString
}
