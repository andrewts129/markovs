package io.andrewsmith.markovs

import cats.effect.IO
import fs2.Stream
import io.andrewsmith.markovs.Model.selectRandomWeighted
import io.andrewsmith.markovs.processing.PostProcessing.detokenize
import io.andrewsmith.markovs.processing.PreProcessing
import io.andrewsmith.markovs.processing.PreProcessing.PosToken
import io.andrewsmith.markovs.schema.{DictSchema, FileSchema, Schema}

import java.nio.file.Path
import scala.util.Random

object Model {
  def memory(corpus: Stream[IO, String], n: Int): Stream[IO, Model[PosToken, DictSchema]] = {
    val processed = corpus.map(document => PreProcessing.all(document, 2, n + 2))
    val schema = DictSchema(processed)
    schema.map(new Model[PosToken, DictSchema](_, n))
  }

  def persistent(filePath: Path, corpus: Stream[IO, String], n: Int): Stream[IO, Model[PosToken, FileSchema]] = {
    val processed = corpus.map(document => PreProcessing.all(document, 2, n + 2))
    val schema = FileSchema(filePath, processed)
    schema.map(new Model[PosToken, FileSchema](_, n))
  }

  def load(filePath: Path): Stream[IO, Model[PosToken, FileSchema]] = {
    val schema = FileSchema[PosToken](filePath)
    Stream.eval(schema.n).map(
      n => new Model(schema, n)
    )
  }

  def generateText[T[_] <: Schema[_]](model: Model[PosToken, T]): Stream[IO, String] = {
    model.generate.through(detokenize)
  }

  def generateWords[T[_] <: Schema[_]](model: Model[PosToken, T]): Stream[IO, String] = {
    generateText(model).flatMap(sentence => Stream.emits(sentence.split(' ')))
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

  def +(other: Model[S, T]): IO[Model[S, T]] = {
    (this.schema + other.schema).map(
      newSchema => new Model[S, T](newSchema, math.max(this.n, other.n), randomSeed)
    )
  }

  def generate: Stream[IO, S] = {
    Stream.eval(schema.getSeed(random)).flatMap {
      case Some(seed) => generate(Stream.emit(seed))
      case None => Stream.empty
    }
  }

  def generate(seed: Stream[IO, S]): Stream[IO, S] = {
    seed ++ nextStreaming(seed)
  }

  private def next(tokens: Stream[IO, S]): IO[Option[S]] = {
    val possibleNextTokens = Stream.range(this.n, 0 , -1).evalMap(n => nextWithN(tokens, n)).unNone
    possibleNextTokens.head.compile.toList.map(_.headOption)
  }

  private def nextWithN(tokens: Stream[IO, S], n: Int): IO[Option[S]] = {
    tokens.takeRight(n).compile.toVector.flatMap {
      case Vector() => IO.pure { None }
      case lastTokens => schema.successorsOf(lastTokens).map {
        case Some(successors) => Some(selectRandomWeighted(successors, random))
        case None => None
      }
    }
  }

  private def nextStreaming(tokens: Stream[IO, S]): Stream[IO, S] = {
    Stream.eval(next(tokens)).flatMap {
      case Some(nextToken) =>
        val nextTokenAsStream = Stream.emit(nextToken)
        val newSeed = (tokens ++ nextTokenAsStream).takeRight(n)
        nextTokenAsStream ++ nextStreaming(newSeed)
      case None => Stream.empty
    }
  }

  override def toString: String = schema.toString
}
