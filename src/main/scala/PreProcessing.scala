import cats.effect.IO
import fs2.{Pure, Stream}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.tokenize.DetokenizationDictionary.Operation
import opennlp.tools.tokenize.{DetokenizationDictionary, DictionaryDetokenizer, SimpleTokenizer}

object PreProcessing {
  case class PosToken(token: String, pos: String) {
    override def toString: String = token
  }

  private lazy val tokenizer = SimpleTokenizer.INSTANCE
  private lazy val taggerModel = new POSModel(getClass.getResourceAsStream("en-pos-maxent.bin"))  // TODO download this
  private lazy val tagger = new POSTaggerME(taggerModel)

  private val NO_SPACE_BEFORE = Array(".", ",", "!", "?", ";", ":", "%", ")", "]", "}", "”")
  private val NO_SPACE_AFTER = Array("(", "[", "{", "$", "“", "#")
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

  def all(string: String, ngramSize: Int): Stream[Pure, Vector[PosToken]] = {
    asNGrams(posTag(tokenize(string)), ngramSize)
  }

  def all(string: String, minNgramSize: Int, maxNgramSize: Int): Stream[Pure, Vector[PosToken]] = {
    val tokens = posTag(tokenize(string))
    Stream.range(minNgramSize, maxNgramSize).flatMap(asNGrams(tokens, _))
  }

  def asNGrams[S](tokens: Stream[Pure, S], n: Int): Stream[Pure, Vector[S]] = {
    tokens.sliding(n).map(_.toVector)
  }

  def tokenize(string: String): Stream[Pure, String] = {
    Stream.emits(tokenizer.tokenize(string))
  }

  def detokenize(tokens: Vector[String]): String = {
    detokenizer.detokenize(tokens.toArray, null)
  }

  def detokenize(posTokens: Stream[IO, PosToken]): Stream[IO, String] = {
    posTokens.map(_.token).chunkN(30).map(chunk => detokenize(chunk.toVector))
  }

  def posTag(tokens: Stream[Pure, String]): Stream[Pure, PosToken] = {
    val tags = Stream.emits(tagger.tag(tokens.toVector.toArray))
    tokens.zip(tags).map { case (token, tag) => PosToken(token, tag) }
  }
}
