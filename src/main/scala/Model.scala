import Model.{Weights, selectRandomWeighted}
import fs2.{Pure, Stream}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.util.Random

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

  private def selectRandomWeighted(itemsWeighted: Map[String, Int], random: Random): String = {
    val sortedByWeight = itemsWeighted.toVector.sortBy(_._2)

    val items = sortedByWeight.map(_._1)
    val weights = sortedByWeight.map(_._2)
    val cumulativeWeights = weights.scanLeft(0)(_ + _).tail

    val randomNumber = random.between(0.0, cumulativeWeights.last)
    items.zip(cumulativeWeights).find { pair => randomNumber <= pair._2 }.get._1
  }
}

class Model private(val weights: Weights, val seed: Option[Long] = None) {
  private val random = seed match {
    case Some(value) => new Random(value)
    case None => new Random()
  }

  def +(other: Model): Model = {
    val newWeights = weights.merged(other.weights) { case ((firstWord, thisSuccessors), (_, otherSuccessors)) =>
      firstWord -> thisSuccessors.merged(otherSuccessors) { case ((secondWord, thisCount), (_, otherCount)) =>
        secondWord -> (thisCount + otherCount)
      }
    }

    new Model(newWeights)
  }

  @tailrec
  final def generate(seed: Stream[Pure, String]): Stream[Pure, String] = {
    next(seed) match {
      case Stream.empty => seed
      case nextToken => generate(seed ++ nextToken)
    }
  }

  def next(tokens: Stream[Pure, String]): Stream[Pure, String] = {
    tokens.last.toVector.head match {
      case Some(lastToken) => weights.get(lastToken) match {
        case Some(successors) => Stream.emit(selectRandomWeighted(successors, random))
        case None => Stream.empty
      }
      case None => Stream.empty
    }
  }

  override def toString: String = weights.toString
}
