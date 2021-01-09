package schema

import processing.PreProcessing.PosToken
import schema.StringSerializable.syntax._

trait StringSerializable[A] {
  def serialize(a: A): String
}

object StringSerializable {
  implicit val stringIsSerializable: StringSerializable[String] = {
    (str: String) => str.replaceAll("[\n\t]", " ")
  }

  implicit val posTokenIsSerializable: StringSerializable[PosToken] = {
    (posToken: PosToken) => posToken.token.serialize + "\t" + posToken.pos.serialize
  }

  implicit def vectorIsSerializable[T : StringSerializable]: StringSerializable[Vector[T]] = {
    // Add a trailing newline to differentiate vectors of size one from lone S objects
    (vector: Vector[T]) => vector.map(_.serialize).mkString("\n") ++ "\n"
  }

  object syntax {
    implicit class StringSerializableOps[A](a: A) {
      def serialize(implicit stringSerializableInstance: StringSerializable[A]): String = {
        stringSerializableInstance.serialize(a)
      }
    }
  }
}
