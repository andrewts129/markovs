import Model.selectRandomWeighted
import processing.PreProcessing.PosToken
import fs2.{Pure, Stream}
import processing.PreProcessing
import schema.{DictSchema, FileSchema, Schema}

import scala.util.Random

object Model {
  def memory[F[_]](corpus: Stream[F, String], n: Int): Stream[F, Model[PosToken, DictSchema]] = {
    corpus.map(Model.memory(_, n)).reduce(_ + _)
  }

  def memory(document: String, n: Int): Model[PosToken, DictSchema] = {
    val processedTokens = PreProcessing.all(document, 2, n + 2)
    new Model(DictSchema(processedTokens), n)
  }

  def persistent[F[_]](filePath: String, corpus: Stream[F, String], n: Int): Stream[F, Model[PosToken, FileSchema]] = {
    memory(corpus, n).map(
      memoryModel => new Model(memoryModel.schema.toFileSchema(filePath), n)
    )
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

class Model[S, T[_] <: Schema[_]] private(val schema: Schema[S], val n: Int, val randomSeed: Option[Long] = None) {
  private val random = randomSeed match {
    case Some(value) => new Random(value)
    case None => new Random()
  }

  def +(other: Model[S, T]): Model[S, T] = {
    new Model[S, T](this.schema + other.schema, math.max(this.n, other.n), randomSeed)
  }

  def generate: Stream[Pure, S] = {
    schema.getSeed(random) match {
      case Some(seed) => generate(Stream.emit(seed))
      case None => Stream.empty
    }
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
