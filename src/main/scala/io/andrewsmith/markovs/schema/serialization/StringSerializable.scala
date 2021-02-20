package io.andrewsmith.markovs.schema.serialization

import io.andrewsmith.markovs.processing.PreProcessing.PosToken
import io.andrewsmith.markovs.schema.serialization.StringSerializable.syntax._

trait StringSerializable[A] {
  def serialize(a: A): String
}

object StringSerializable {
  implicit val stringIsSerializable: StringSerializable[String] = {
    (str: String) => str.replace('\n', ' ').replace('\t', ' ')
  }

  implicit val posTokenIsSerializable: StringSerializable[PosToken] = {
    (posToken: PosToken) => posToken.token.serialize + "\t" + posToken.pos.serialize
  }

  implicit def vectorIsSerializable[S : StringSerializable]: StringSerializable[Vector[S]] = {
    // Add a trailing newline to differentiate vectors of size one from lone S objects
    (vector: Vector[S]) => vector.map(_.serialize).mkString("\n") ++ "\n"
  }

  object syntax {
    implicit class StringSerializableOps[A](a: A) {
      def serialize(implicit stringSerializableInstance: StringSerializable[A]): String = {
        stringSerializableInstance.serialize(a)
      }
    }
  }
}
