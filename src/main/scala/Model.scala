import Model.selectRandomWeighted
import fs2.{Pure, Stream}
import opennlp.tools.tokenize.SimpleTokenizer

import scala.util.Random

object Model {
  def apply[F[_]](corpus: Stream[F, String], n: Int): Stream[F, Model[String]] = {
    corpus.map(Model(_, n)).reduce(_ + _)
  }

  def apply(document: String, n: Int): Model[String] = {
    val tokens = tokenize(document)
    val nGrams = Stream.range(2, n + 2).flatMap(asNGrams(tokens, _))
    new Model(Schema(nGrams), n)
  }

  private def asNGrams(tokens: Stream[Pure, String], n: Int): Stream[Pure, Vector[String]] = {
    tokens.sliding(n).map(_.toVector)
  }

  private def tokenize(string: String): Stream[Pure, String] = {
    Stream.emits(SimpleTokenizer.INSTANCE.tokenize(string))
  }

  private def selectRandomWeighted[S](itemsWeighted: Map[S, Int], random: Random): S = {
    val sortedByWeight = itemsWeighted.toVector.sortBy(_._2)

    val items = sortedByWeight.map(_._1)
    val weights = sortedByWeight.map(_._2)
    val cumulativeWeights = weights.scanLeft(0)(_ + _).tail

    val randomNumber = random.between(0.0, cumulativeWeights.last)
    items.zip(cumulativeWeights).find { pair => randomNumber <= pair._2 }.get._1
  }
}

class Model[S] private(val schema: Schema[S], val n: Int, val randomSeed: Option[Long] = None) {
  private val random = randomSeed match {
    case Some(value) => new Random(value)
    case None => new Random()
  }

  def +(other: Model[S]): Model[S] = {
    new Model(this.schema + other.schema, math.max(this.n, other.n), randomSeed)
  }

  def generate(seed: Stream[Pure, S]): Stream[Pure, S] = {
    seed ++ nextStreaming(seed)
  }

  private def next(tokens: Stream[Pure, S]): Option[S] = {
    val possibleNextTokens = Stream.range(this.n, 0 , -1).map(n => nextWithN(tokens, n)).unNone
    possibleNextTokens.head.toVector match {
      case nextToken +: _ => Some(nextToken)
      case Vector() => None
    }
  }

  private def nextWithN(tokens: Stream[Pure, S], n: Int): Option[S] = {
    tokens.takeRight(n).toVector match {
      case Vector() => None
      case lastTokens => schema.successorsOf(lastTokens) match {
        case Some(successors) => Some(selectRandomWeighted(successors, random))
        case None => None
      }
    }
  }

  private def nextStreaming(tokens: Stream[Pure, S]): Stream[Pure, S] = {
    next(tokens) match {
      case Some(nextToken) =>
        val nextTokenAsStream = Stream.emit(nextToken)
        nextTokenAsStream ++ nextStreaming(tokens ++ nextTokenAsStream)
      case None => Stream.empty
    }
  }

  override def toString: String = schema.toString
}
