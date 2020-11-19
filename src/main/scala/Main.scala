import java.nio.file.{Path, Paths}

import cats.effect.{Blocker, ExitCode, IO, IOApp}
import fs2.{Stream, io, text}

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val input = Stream.resource(Blocker[IO]).flatMap { blocker =>
      io.file.readAll[IO](inputPath, blocker, 1024)
        .through(text.utf8Decode)
        .through(text.lines)
    }

    val model = Model[IO](input)
    val seed = Stream.emit("hello")
    model.flatMap(_.generate(seed)).intersperse(" ").map(print(_)).compile.drain.as(ExitCode.Success)
  }

  private def inputPath: Path = Paths.get(getClass.getResource("test.txt").toURI)
}
