package schema

import java.nio.file.Path
import scala.collection.immutable.HashMap
import scala.util.Random

class FileSchema[S] private(val file: Path) extends Schema[S] {
  override def +(other: Schema[S]): Schema[S] = ???

  override def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]] = ???

  override def getSeed(random: Random): Option[S] = ???

  override def toDictSchema: DictSchema[S] = ???

  override def toFileSchema: FileSchema[S] = this
}
