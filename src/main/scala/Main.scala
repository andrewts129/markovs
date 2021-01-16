import cats.effect.{Blocker, ExitCode, IO}
import com.monovore.decline._
import com.monovore.decline.effect.CommandIOApp
import fs2.{Stream, io, text}
import processing.PostProcessing.detokenize
import cats.implicits._
import cats.data.Validated

import java.nio.file.Path

object Main extends CommandIOApp(
  name = "markovs",
  header = "Markov chain-based text generation",
  version = "0.1"
) {
  override def main: Opts[IO[ExitCode]] = {
    val inputSourceFileOpt = Opts.option[Path]("parse", short = "p", help = "Text file to parse").orNone
    val outputSourceFileOpt = Opts.option[Path]("output", short = "o", help = "File to dump schema to").withDefault(Path.of("default.schema"))
    val inputSchemaFileOpt = Opts.option[Path]("read", short = "r", help = "Schema file to load").orNone
    val ngramSizeOpt = Opts.option[Int]("ngram-size", short = "n", help = "Max ngram size to use").withDefault(2)

    val validatedInput = (inputSourceFileOpt, inputSchemaFileOpt).mapN {
      (inputSourceFile, inputSchemaFile) => {
        if (inputSourceFile.isDefined && inputSchemaFile.isDefined) {
          Validated.invalid("Cannot read from both an input schema and text file")
        } else if (inputSourceFile.isEmpty && inputSchemaFile.isEmpty) {
          Validated.invalid("Must provide either a schema file or text file as input")
        } else {
          Validated.valid(inputSourceFile, inputSchemaFile)
        }
      }
    }

    (validatedInput, outputSourceFileOpt, ngramSizeOpt).mapN {
      case (Validated.Valid(bothInputFiles), outputSourceFile, ngramSize) =>
        val model = bothInputFiles match {
          case (Some(inputSourceFile), None) =>
            val input = Stream.resource(Blocker[IO]).flatMap { blocker =>
              io.file.readAll[IO](inputSourceFile, blocker, 1024)
                .through(text.utf8Decode)
                .through(text.lines)
            }

            Model.persistent(outputSourceFile, input, ngramSize)
          case (None, Some(inputSchemaFile)) =>
            Model.load(inputSchemaFile)
          case _ => throw new Exception("Illegal arguments")
        }

        val generatedTokens = model.flatMap(_.generate)
        generatedTokens.through(detokenize).intersperse(" ").map(print(_)).compile.drain.as(ExitCode.Success)
      case (Validated.Invalid(errorMessage), _, _) => IO {
        println(errorMessage)
        ExitCode.Error
      }
    }
  }
}
