package markovs.processing

import cats.effect.IO
import cats.implicits._
import fs2.{Chunk, Stream}
import opennlp.tools.tokenize.DetokenizationDictionary.Operation
import opennlp.tools.tokenize.{DetokenizationDictionary, DictionaryDetokenizer}
import markovs.processing.PreProcessing.PosToken

object PostProcessing {
  private val NO_SPACE_BEFORE = Array(".", ",", "!", "?", ";", ":", "%", ")", "]", "}", "”")
  private val NO_SPACE_AFTER = Array("(", "[", "{", "$", "“", "#", "@")
  private val NO_SPACE_EITHER = Array("-", "_", "’")
  private val ALTERNATING = Array("\"", "'")

  private lazy val detokenizer = {
    val instructions: Array[(String, Operation)] =
      NO_SPACE_BEFORE.map((_, Operation.MOVE_LEFT)) ++
        NO_SPACE_AFTER.map((_, Operation.MOVE_RIGHT)) ++
        NO_SPACE_EITHER.map((_, Operation.MOVE_BOTH)) ++
        ALTERNATING.map((_, Operation.RIGHT_LEFT_MATCHING))

    val dict = new DetokenizationDictionary(
      instructions.map(_._1),
      instructions.map(_._2)
    )

    new DictionaryDetokenizer(dict)
  }

  def detokenize(tokens: Vector[String]): String = {
    detokenizer.detokenize(tokens.toArray, null)
  }

  def detokenize(posTokens: Stream[IO, PosToken]): Stream[IO, String] = {
    sentences(posTokens).map(sentence => {
      val tokens = sentence.map(_.token)
      detokenize(tokens.toVector)
    })
  }

  private def sentences(posTokens: Stream[IO, PosToken]): Stream[IO, Chunk[PosToken]] = {
    val groups = posTokens.groupAdjacentBy(_.pos != ".")

    groups.zipWithNext.map {
      case ((_, firstChunk), maybeSecond) => maybeSecond match {
        case Some((isSecondSentence, secondChunk)) =>
          if (isSecondSentence) None else Some(Chunk.concat(Seq(firstChunk, secondChunk)))
        case None => Some(firstChunk)
      }
    }.unNone
  }
}
