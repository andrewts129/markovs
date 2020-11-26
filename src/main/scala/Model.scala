import Model.selectRandomWeighted
import fs2.{Pure, Stream}

import scala.util.Random

object Model {
  def apply[F[_]](corpus: Stream[F, String]): Stream[F, Model] = {
    corpus.map(Model(_)).reduce(_ + _)
  }

  def apply(document: String): Model = {
    new Model(Schema(asNGrams(tokenize(document))))
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

class Model private(val schema: Schema, val seed: Option[Long] = None) {
  private val random = seed match {
    case Some(value) => new Random(value)
    case None => new Random()
  }

  def +(other: Model): Model = {
    new Model(this.schema + other.schema, seed)
  }

  def generate(seed: Stream[Pure, String]): Stream[Pure, String] = {
    seed ++ nextStreaming(seed)
  }

  private def next(tokens: Stream[Pure, String]): Option[String] = {
    tokens.last.toVector.head match {
      case Some(lastToken) => schema.successorsOf(lastToken) match {
        case Some(successors) => Some(selectRandomWeighted(successors, random))
        case None => None
      }
      case None => None
    }
  }

  private def nextStreaming(tokens: Stream[Pure, String]): Stream[Pure, String] = {
    next(tokens) match {
      case Some(nextToken) =>
        val nextTokenAsStream = Stream.emit(nextToken)
        nextTokenAsStream ++ nextStreaming(tokens ++ nextTokenAsStream)
      case None => Stream.empty
    }
  }

  override def toString: String = schema.toString
}
