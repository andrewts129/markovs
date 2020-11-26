import Model.selectRandomWeighted
import fs2.{Pure, Stream}

import scala.util.Random

object Model {
  def apply[F[_]](corpus: Stream[F, String], n: Int): Stream[F, Model] = {
    corpus.map(Model(_, n)).reduce(_ + _)
  }

  def apply(document: String, n: Int): Model = {
    val schema = Schema(asNGrams(tokenize(document), n + 1))
    new Model(schema, n)
  }

  private def asNGrams(tokens: Stream[Pure, String], n: Int): Stream[Pure, Vector[String]] = {
    tokens.sliding(n).map(_.toVector)
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

class Model private(val schema: Schema, val n: Int, val randomSeed: Option[Long] = None) {
  private val random = randomSeed match {
    case Some(value) => new Random(value)
    case None => new Random()
  }

  def +(other: Model): Model = {
    new Model(this.schema + other.schema, math.max(this.n, other.n), randomSeed)
  }

  def generate(seed: Stream[Pure, String]): Stream[Pure, String] = {
    seed ++ nextStreaming(seed)
  }

  private def next(tokens: Stream[Pure, String]): Option[String] = {
    tokens.takeRight(this.n).toVector match {
      case Vector() => None
      case lastTokens => schema.successorsOf(lastTokens) match {
        case Some(successors) => Some(selectRandomWeighted(successors, random))
        case None => None
      }
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
