package schema

import processing.PreProcessing.PosToken

trait StringDeserializable[A] {
  def deserialize[T : StringSerializable](a: A): T
}

object StringDeserializable {
  implicit def stringIsDeserializable: StringDeserializable[String] = new StringDeserializable[String] {
    override def deserialize[T : StringSerializable](str: String): T = {
      def lineToPosToken(line: String): PosToken = {
        val Array(token, pos, _*) = line.split("\t", 2)
        PosToken(token, pos)
      }

      val result = if (str.contains("\n")) {
        val entries = str.split("\n").toVector
        if (entries.head.contains("\t")) {
          entries.map(lineToPosToken)
        } else {
          entries
        }
      } else if (str.contains("\t")) {
        lineToPosToken(str)
      } else {
        str
      }

      result.asInstanceOf[T]
    }
  }

  object syntax {
    implicit class StringDeserializableOps[A](a: A) {
      def deserialize[T : StringSerializable](implicit stringDeserializableInstance: StringDeserializable[A]): T = {
        stringDeserializableInstance.deserialize[T](a)
      }
    }
  }
}
