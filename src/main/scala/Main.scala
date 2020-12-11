import PostProcessing.detokenize

import java.nio.file.{Path, Paths}
import PreProcessing.{posTag, tokenize}
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import fs2.{Stream, io, text}

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val input = Stream.resource(Blocker[IO]).flatMap { blocker =>
      io.file.readAll[IO](inputPath, blocker, 1024)
        .through(text.utf8Decode)
        .through(text.lines)
    }

    val model = Model[IO](input.take(2000), 2)
    val seed = posTag(tokenize("And"))
    val generatedTokens = model.flatMap(_.generate(seed))

    generatedTokens.through(detokenize).intersperse("|").map(print(_)).compile.drain.as(ExitCode.Success)
  }

  private def inputPath: Path = Paths.get(getClass.getResource("nyt.txt").toURI)
}
