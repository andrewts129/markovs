import fs2.{Pure, Stream}
import opennlp.tools.tokenize.SimpleTokenizer

object PreProcessing {
  private lazy val tokenizer = SimpleTokenizer.INSTANCE

  def asNGrams(tokens: Stream[Pure, String], n: Int): Stream[Pure, Vector[String]] = {
    tokens.sliding(n).map(_.toVector)
  }

  def tokenize(string: String): Stream[Pure, String] = {
    Stream.emits(tokenizer.tokenize(string))
  }
}
