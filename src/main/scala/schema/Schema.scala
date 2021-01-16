package schema

import cats.effect.IO

import java.nio.file.Path
import scala.collection.immutable.HashMap
import scala.util.Random

trait Schema[S] {
  def +(other: Schema[S]): IO[Schema[S]]

  def successorsOf(tokens: Vector[S]): IO[Option[HashMap[S, Int]]]

  def getSeed(random: Random): IO[Option[S]]

  def toDictSchema: IO[DictSchema[S]]

  def toFileSchema(filePath: Path): IO[FileSchema[S]]

  def n: IO[Int]
}
