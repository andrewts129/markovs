package schema

import scala.collection.immutable.HashMap
import scala.util.Random

trait Schema[S] {
  def +(other: Schema[S]): Schema[S]

  def successorsOf(tokens: Vector[S]): Option[HashMap[S, Int]]

  def getSeed(random: Random): Option[S]

  def toDictSchema: DictSchema[S]

  def toFileSchema(filePath: String): FileSchema[S]
}
