import PreProcessing.PosToken
import cats.effect.IO
import fs2.Stream
import opennlp.tools.tokenize.DetokenizationDictionary.Operation
import opennlp.tools.tokenize.{DetokenizationDictionary, DictionaryDetokenizer}

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
    // TODO chunk by sentences
    posTokens.map(_.token).chunkN(30).map(chunk => detokenize(chunk.toVector))
  }
}
